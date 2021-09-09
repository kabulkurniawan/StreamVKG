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

    public static void main(String[] args) throws InterruptedException, IOException, ConfigurationException {
        ARQ.init();
        EngineConfiguration ec = new EngineConfiguration("csparql.properties");
        SDSConfiguration config = new SDSConfiguration("csparql.properties");

        PrintWriter wr = createTcpClient("localhost",8880);
        //init tcp server
        TcpSocketStream wr1 = createTcpServer("http://streamreasoning.org/csparql/streams/stream2",7770);
        TcpSocketStream wr2 = createTcpServer("http://streamreasoning.org/csparql/streams/stream3",7771);
        TcpSocketStream wr3 = createTcpServer("http://streamreasoning.org/csparql/streams/stream4",7772);
        TcpSocketStream wr4 = createTcpServer("http://streamreasoning.org/csparql/streams/stream5",7773);


        //init engine for Query1
        CSPARQLEngine sr = new CSPARQLEngine(0, ec);

        //register streams
        registerStream(sr,wr1);
        registerStream(sr,wr2);
        registerStream(sr,wr3);
        registerStream(sr,wr4);

        //register queries
       registerQuery(sr, config, "rtgp-q2", ".rspql",wr);

        //init engine for Query2
        CSPARQLEngine sr2 = new CSPARQLEngine(0, ec);

        //register streams2
        registerStream(sr2,wr1);
        registerStream(sr2,wr2);
        registerStream(sr2,wr3);
        registerStream(sr2,wr4);

        //register queries2
        registerQuery(sr2, config, "rtgp-q1", ".rspql",wr);



    }

    public static String getQuery(String queryName, String suffix) throws IOException {
        File file = new File(queryName + suffix);
        return FileUtils.readFileToString(file, StandardCharsets.UTF_8).replace("\r","");
    }

    public static void registerStream(CSPARQLEngine sr, TcpSocketStream writer){
        DataStreamImpl<Graph> register;
        register = sr.register(writer);
        writer.setWritable(register);
    }

    public static void registerQuery(CSPARQLEngine sr, SDSConfiguration config, String queryName, String suffix, PrintWriter wr) throws IOException, ConfigurationException {
        ContinuousQueryExecution cqe = sr.register(getQuery(queryName, suffix), config);
        cqe.add(new StreamOutputFormatter("TURTLE", true,wr));
    }


    public static PrintWriter createTcpClient(String host, int port) throws IOException {
        Socket cs = new Socket(host,port);
        PrintWriter writer = new PrintWriter(cs.getOutputStream(),true);
        return writer;
    }

    public static TcpSocketStream createTcpServer(String stream, int port){
        TcpSocketStream writer = new TcpSocketStream("Writer", stream, port);
        (new Thread(writer)).start();
        return writer;
    }
}
