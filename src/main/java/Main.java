import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
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
        BufferedReader in = new BufferedReader(
            new InputStreamReader(clientSocket.getInputStream()));
        BufferedWriter out = new BufferedWriter(
            new OutputStreamWriter(clientSocket.getOutputStream()));
        String content;
        while ((content = in.readLine()) != null) {
          System.out.println("Req: " + content);
          if ("PING".equals(content)) {
            out.write("+PONG\r\n");
            out.flush();
          } else {
            System.out.println("Not ping: " + content);
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
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
