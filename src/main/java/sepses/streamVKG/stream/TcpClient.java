package sepses.streamVKG.stream;
import java.io.*;
import java.net.*;


/**
 * Created by Kabul on 22/07/2021.
 */


public class TcpClient {

    public static void main(String[] args) {
        try {
            Socket s = new Socket("localhost", 6660);
            InputStream input = s.getInputStream();
            BufferedReader in = new BufferedReader(new InputStreamReader(input));
            String line;
            while ((line = in.readLine()) != null) {
                System.out.println(line);
            }

        } catch (Exception e) {
            System.out.println(e);
        }
    }
  }

