import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

  private static Map<String, String> mp = new HashMap<>();
  private static Map<String, Long> expireTime = new HashMap<>();

  private static Map<String, String> config = new HashMap<>();

  private static class Worker implements Runnable {
    private Socket clientSocket;

    private Worker(Socket clientSocket) {
      this.clientSocket = clientSocket;
    }

    // Reads the bulk string based on RESP protocol
    private String readBulkString(BufferedReader bufferedReader) throws IOException {
      String line = bufferedReader.readLine();
      if (line.startsWith("$")) {
        int length = Integer.parseInt(line.substring(1));
        if (length > 0) {
          char[] data = new char[length];
          bufferedReader.read(data, 0, length);
          bufferedReader.readLine(); // Read the trailing newline
          return new String(data);
        }
      }
      return null;
    }

    @Override
    public void run() {
      try {
        OutputStream outputStream = clientSocket.getOutputStream();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

        while (true) {
          String line = bufferedReader.readLine();
          if (line == null) {
            break; // End of input, close the connection
          }

          if (line.startsWith("*")) {
            // This is an array, read the number of elements
            int numElements = Integer.parseInt(line.substring(1));
            if (numElements < 1) {
              outputStream.write("-ERR Invalid RESP Array\r\n".getBytes());
              outputStream.flush();
              continue;
            }

            String command = readBulkString(bufferedReader);
            if ("PING".equalsIgnoreCase(command)) {
              handlePing(outputStream);
            } else if ("SET".equalsIgnoreCase(command)) {
              handleSet(bufferedReader, outputStream, numElements);
            } else if ("GET".equalsIgnoreCase(command)) {
              handleGet(bufferedReader, outputStream);
            } else if ("ECHO".equalsIgnoreCase(command)) {
              handleEcho(bufferedReader, outputStream);
            } else if ("CONFIG".equalsIgnoreCase(command)) {
              rdbFileConfig(bufferedReader, outputStream);
            } else if ("KEYS".equalsIgnoreCase(command)) {
              readKeysFromRDBFileConfig(bufferedReader, outputStream);
            } else {
              outputStream.write("-ERR Unknown Command\r\n".getBytes());
              outputStream.flush();
            }
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    private void handleSet(BufferedReader bufferedReader, OutputStream outputStream, int numElements)
        throws IOException {
      if (numElements < 3) {
        outputStream.write("-ERR Invalid SET command format\r\n".getBytes());
        outputStream.flush();
        return;
      }

      String key = readBulkString(bufferedReader); // Key
      String value = readBulkString(bufferedReader); // Value

      if (key == null || value == null) {
        outputStream.write("-ERR Invalid Key or Value\r\n".getBytes());
        outputStream.flush();
        return;
      }

      long expiration = 0;
      if (numElements == 5) { // Check for expiration parameter (e.g., px)
        String option = readBulkString(bufferedReader); // Should be "px"
        String expirationString = readBulkString(bufferedReader); // Expiration time in milliseconds

        if ("px".equalsIgnoreCase(option)) {
          try {
            expiration = Long.parseLong(expirationString);
          } catch (NumberFormatException e) {
            outputStream.write("-ERR Invalid expiration time\r\n".getBytes());
            outputStream.flush();
            return;
          }
        }
      }

      mp.put(key, value);
      if (expiration > 0) {
        expireTime.put(key, System.currentTimeMillis() + expiration);
      }

      outputStream.write("+OK\r\n".getBytes());
      outputStream.flush();
    }

    private void handleGet(BufferedReader bufferedReader, OutputStream outputStream) throws IOException {
      String key = readBulkString(bufferedReader);
      if (key == null) {
        outputStream.write("-ERR Invalid Key\r\n".getBytes());
        outputStream.flush();
        return;
      }

      // Check if the key has expired
      if (expireTime.containsKey(key) && System.currentTimeMillis() > expireTime.get(key)) {
        mp.remove(key);
        expireTime.remove(key);
      }

      String value = mp.get(key);
      if (value != null) {
        outputStream.write(("+" + value + "\r\n").getBytes());
      } else {
        outputStream.write("$-1\r\n".getBytes());
      }
      outputStream.flush();
    }

    public void handleEcho(BufferedReader bufferedReader, OutputStream outputStream) throws IOException {
      String value = readBulkString(bufferedReader);
      outputStream.write(("+" + value + "\r\n").getBytes());
      outputStream.flush();
    }

    public void handlePing(OutputStream outputStream) throws IOException {
      outputStream.write("+PONG\r\n".getBytes());
      outputStream.flush();
    }

    public void rdbFileConfig(BufferedReader bufferedReader, OutputStream outputStream) throws IOException {
      String getCmd = readBulkString(bufferedReader);
      if ("GET".equalsIgnoreCase(getCmd)) {
        String key = readBulkString(bufferedReader);
        String value = config.get(key);

        if (value == null) {
          outputStream.write("$-1\r\n".getBytes());
        } else {
          outputStream.write(("*2\r\n$3\r\ndir\r\n$" + value.length() + "\r\n" + value + "\r\n").getBytes());
        }
      } else {
        outputStream.write("-1ERR not getcmd\r\n".getBytes());
      }

      outputStream.flush();
    }

    public void readKeysFromRDBFileConfig(BufferedReader bufferedReader, OutputStream outputStream) throws IOException {
      String dir = Main.config.get("dir");
      String dbfilename = Main.config.get("dbfilename");
      String key = "foo";
      try (InputStream fis = new FileInputStream(new File(dir, dbfilename))) {
        byte[] redis = new byte[5];
        byte[] version = new byte[4];
        fis.read(redis);
        fis.read(version);
        System.out.println("Magic String = " +
            new String(redis, StandardCharsets.UTF_8));
        System.out.println("Version = " +
            new String(version, StandardCharsets.UTF_8));
        int b;
        header: while ((b = fis.read()) != -1) {
          switch (b) {
            case 0xFF:
              System.out.println("EOF");
              break;
            case 0xFE:
              System.out.println("SELECTDB");
              break;
            case 0xFD:
              System.out.println("EXPIRETIME");
              break;
            case 0xFC:
              System.out.println("EXPIRETIMEMS");
              break;
            case 0xFB:
              System.out.println("RESIZEDB");
              b = fis.read();
              fis.readNBytes(lengthEncoding(fis, b));
              fis.readNBytes(lengthEncoding(fis, b));
              break header;
            case 0xFA:
              System.out.println("AUX");
              break;
          }
        }
        System.out.println("header done");
        // now key value pairs
        while ((b = fis.read()) != -1) { // value type
          System.out.println("value-type = " + b);
          b = fis.read();
          System.out.println("value-type = " + b);
          // b = fis.read();
          // System.out.println("value-type
          // = " + b);
          System.out.println(" b = " + Integer.toBinaryString(b));
          System.out.println("reading keys");
          int strLength = lengthEncoding(fis, b);
          b = fis.read();
          System.out.println("strLength == " + strLength);
          if (strLength == 0) {
            strLength = b;
          }
          System.out.println("strLength == " + strLength);
          byte[] bytes = fis.readNBytes(strLength);
          key = new String(bytes);
          break;
        }
      }
      outputStream.write(("*1\r\n$" + key.length() + "\r\n" + key + "\r\n").getBytes());
      outputStream.flush();
    }

    private static int lengthEncoding(InputStream is, int b) throws IOException {
      int length = 100;
      int first2bits = b & 11000000;
      if (first2bits == 0) {
        System.out.println("00");
        length = 0;
      } else if (first2bits == 128) {
        System.out.println("01");
        length = 2;
      } else if (first2bits == 256) {
        System.out.println("10");
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.put(is.readNBytes(4));
        buffer.rewind();
        length = 1 + buffer.getInt();
      } else if (first2bits == 256 + 128) {
        System.out.println("11");
        length = 1; // special format
      }
      return length;
    }

  }

  public static void main(String[] args) throws IOException {

    for (int i = 0; i < args.length; ++i) {
      String arg = args[i];
      if (arg.equals("--dir")) {
        config.put("dir", args[++i]);
      }

      if (arg.equals("--dbfilename")) {
        config.put("dbfilename", args[++i]);
      }

    }
    System.out.println("Server started on port 6379");

    int port = 6379;

    try (ServerSocket serverSocket = new ServerSocket(port)) {
      serverSocket.setReuseAddress(true);

      // Use a thread pool for handling client connections
      ExecutorService threadPool = Executors.newFixedThreadPool(10);

      while (true) {
        Socket clientSocket = serverSocket.accept();
        threadPool.execute(new Worker(clientSocket));
      }
    } catch (IOException e) {
      System.out.println("Server error: " + e.getMessage());
    }
  }
}
