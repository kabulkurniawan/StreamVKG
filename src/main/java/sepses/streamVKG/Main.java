package sepses.streamVKG;


import it.polimi.sr.rsp.csparql.engine.CSPARQLEngine;
import it.polimi.yasper.core.engine.config.EngineConfiguration;
import it.polimi.yasper.core.querying.ContinuousQueryExecution;
import it.polimi.yasper.core.sds.SDSConfiguration;
import it.polimi.yasper.core.stream.data.DataStreamImpl;
import org.apache.jena.query.ARQ;
import sepses.streamVKG.stream.StreamOutputFormatter;
import sepses.streamVKG.stream.TcpSocketStream;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.jena.graph.Graph;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Created by Kabul on 24/07/2021.
 */
public class Main {

    static CSPARQLEngine sr;

    public static void main(String[] args) throws InterruptedException, IOException, ConfigurationException {
        ARQ.init();
        EngineConfiguration ec = new EngineConfiguration("csparql.properties");
        sr = new CSPARQLEngine(0, ec);

        SDSConfiguration config = new SDSConfiguration("csparql.properties");

        PrintWriter wr = createTcpClient("localhost",8880);

        //register streams
        registerStream(sr,"http://streamreasoning.org/csparql/streams/stream2",7770);
        registerStream(sr,"http://streamreasoning.org/csparql/streams/stream3",7771);
        registerStream(sr,"http://streamreasoning.org/csparql/streams/stream4",7772);
        registerStream(sr,"http://streamreasoning.org/csparql/streams/stream5",7773);

        //register queries
       registerQuery(sr, config, "rtgp-q1", ".rspql",wr);
       registerQuery(sr, config, "rtgp-q2",".rspql",wr);
        // registerQuery(sr, config, "rtgp-q2",".rspql");
        // registerQuery(sr, config, "rtgp-q2",".rspql");



        //send to another rsp server
        //createTCPClient(out, "localhost",8880 );


    }

    public static String getQuery(String queryName, String suffix) throws IOException {
        File file = new File(queryName + suffix);
        return FileUtils.readFileToString(file, StandardCharsets.UTF_8).replace("\r","");
    }

    public static void registerStream(CSPARQLEngine sr, String stream, int port){
        TcpSocketStream writer;
        DataStreamImpl<Graph> register;

        writer = new TcpSocketStream("Writer", stream, port);
        register = sr.register(writer);
        writer.setWritable(register);
        (new Thread(writer)).start();
    }

    public static void registerQuery(CSPARQLEngine sr, SDSConfiguration config, String queryName, String suffix, PrintWriter wr) throws IOException, ConfigurationException {
        ContinuousQueryExecution cqe;
        cqe = sr.register(getQuery(queryName, suffix), config);
        cqe.add(new StreamOutputFormatter("TURTLE", true,wr));
    }


    public static PrintWriter createTcpClient(String host, int port) throws IOException {
        Socket cs = new Socket(host,port);
        PrintWriter writer = new PrintWriter(cs.getOutputStream(),true);
        return writer;
    }
}
