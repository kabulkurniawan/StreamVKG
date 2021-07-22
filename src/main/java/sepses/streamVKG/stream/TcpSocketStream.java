package sepses.streamVKG.stream;

//import it.polimi.yasper.core.stream.data.DataStreamImpl;
import org.apache.commons.io.IOUtils;
import org.apache.jena.graph.Graph;
import org.apache.jena.rdf.model.*;

import it.polimi.yasper.core.stream.data.DataStreamImpl;

import java.net.*;
import java.io.*;

public class TcpSocketStream extends DataStreamImpl implements Runnable  {
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private DataStreamImpl<Graph> s;
    private String type;
    private int port;
    public TcpSocketStream(String name, String stream_uri, int p) {
        super(stream_uri);
        this.type = name;
        this.port = p;

    }

    public void setWritable(DataStreamImpl<Graph> e) {
        this.s = e;
    }



    public void run() {
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Listening at port: " + port);
            clientSocket = serverSocket.accept();
            System.out.println("New client connected");
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                Model dataModel = ModelFactory.createDefaultModel();
                dataModel.read(IOUtils.toInputStream(line,"UTF-8"), null, "N3");
                this.s.put(dataModel.getGraph(), System.currentTimeMillis());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
