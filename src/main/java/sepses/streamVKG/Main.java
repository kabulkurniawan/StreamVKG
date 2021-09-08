package sepses.streamVKG;


import it.polimi.sr.rsp.csparql.engine.CSPARQLEngine;
import it.polimi.sr.rsp.csparql.sysout.ConstructSysOutDefaultFormatter;
import it.polimi.yasper.core.engine.config.EngineConfiguration;
import it.polimi.yasper.core.querying.ContinuousQuery;
import it.polimi.yasper.core.querying.ContinuousQueryExecution;
import it.polimi.yasper.core.sds.SDSConfiguration;
import it.polimi.yasper.core.stream.data.DataStreamImpl;
import it.polimi.yasper.core.stream.web.WebStream;
import org.apache.jena.query.ARQ;
import sepses.streamVKG.stream.TcpClientSocketStream;
import sepses.streamVKG.stream.TcpSocketStream;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.jena.graph.Graph;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
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

        //register streams
        registerStream(sr,"http://streamreasoning.org/csparql/streams/stream2",7770);
        registerStream(sr,"http://streamreasoning.org/csparql/streams/stream3",7771);
        registerStream(sr,"http://streamreasoning.org/csparql/streams/stream4",7772);
        registerStream(sr,"http://streamreasoning.org/csparql/streams/stream5",7773);

        //register queries
        registerQuery(sr, config, "rtgp-q2",".rspql");
        //registerQuery(sr, config, "rtgp-q1",".rspql");
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

    public static void registerQuery(CSPARQLEngine sr, SDSConfiguration config, String queryName, String suffix) throws IOException, ConfigurationException {

        ContinuousQueryExecution cqe;
        cqe = sr.register(getQuery(queryName, suffix), config);

        cqe.add(new ConstructSysOutDefaultFormatter("TURTLE", true));
        //send to another rsp
        System.out.println("ada lhooooo =>"+cqe.outstream());


        Socket cs = new Socket("localhost",8880);

        PrintWriter writer = new PrintWriter(cs.getOutputStream(),true);
        writer.println(cqe.outstream());

    }

    public static void createTCPClient(WebStream ws, String host, int port) throws IOException {
        TcpClientSocketStream s = new TcpClientSocketStream(ws, host,port);
        (new Thread(s)).start();
    }

}
