package sepses.streamVKG.stream;

import it.polimi.yasper.core.stream.web.WebStream;
import lombok.SneakyThrows;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

public class TcpClientSocketStream  implements Runnable  {
    private WebStream input;
    private String host;
    private int port;

    public TcpClientSocketStream(WebStream input, String host, int port) {
        this.input = input;
        this.host = host;
        this.port = port;

    }

    @SneakyThrows
    public void run() {
        Socket s = new Socket(host,port);
        OutputStream output = s.getOutputStream();
        PrintWriter writer = new PrintWriter(output, true);
        writer.println(input);
    }



}
