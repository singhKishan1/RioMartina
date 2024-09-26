import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

  private static class Worker implements Runnable {
    private Socket clientSocket;

    private Worker(Socket clientSocket) {
      this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
      try {
        OutputStream outputStream = clientSocket.getOutputStream();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        String line;

        while ((line = bufferedReader.readLine()) != null) {
          if (line.startsWith("$")) {
            line = bufferedReader.readLine();
            if (line.startsWith("ECHO")) {
              String echoMsg = bufferedReader.readLine();
              System.out.println("----> " + echoMsg);
              echoMsg = bufferedReader.readLine();
              System.out.println("----> " + echoMsg);
              outputStream.write(("+" + echoMsg + "\r\n").getBytes());
              outputStream.flush();
            } else if (line.startsWith("PING")) {
              outputStream.write(("+PONG\r\n").getBytes());
              outputStream.flush();
            }
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void performEchoOperation(Socket cliSocket) {
    BufferedReader bufferedReader = null;
    OutputStream outputStream = null;

    try {
      outputStream = cliSocket.getOutputStream();
      bufferedReader = new BufferedReader(new InputStreamReader(cliSocket.getInputStream()));
      String line;

      while ((line = bufferedReader.readLine()) != null) {
        if (line.startsWith("$")) {
          line = bufferedReader.readLine();
          if (line.startsWith("ECHO")) {
            String echoMsg = bufferedReader.readLine();
            System.out.println("----> " + echoMsg);
            echoMsg = bufferedReader.readLine();
            System.out.println("----> " + echoMsg);
            outputStream.write(("+" + echoMsg + "\r\n").getBytes());
            outputStream.flush();
          } else if (line.startsWith("PING")) {
            outputStream.write(("+PONG\r\n").getBytes());
            outputStream.flush();
          }
        }
      }
    } catch (IOException e) {
      System.out.println("Exceptoin ouccur --> " + e.getMessage());
    }
  }

  public static void main(String[] args) throws IOException {
    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    System.out.println("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    ServerSocket serverSocket = null;
    int port = 6379;

    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);
      // Wait for connection from client.

      // Socket clientSocket = serverSocket.accept();
      // performEchoOperation(clientSocket);

      ExecutorService es = Executors.newFixedThreadPool(10);
      while (true) {
        Socket clientSocket = serverSocket.accept();
        es.execute(new Worker(clientSocket));
      }
    } catch (IOException e) {
      System.out.println(e.getLocalizedMessage());
    }
  }
}
