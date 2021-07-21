package sepses.streamVKG;



import it.polimi.sr.rsp.csparql.engine.CSPARQLEngine;
import it.polimi.sr.rsp.csparql.sysout.GenericResponseSysOutFormatter;
import it.polimi.yasper.core.engine.config.EngineConfiguration;
import it.polimi.yasper.core.querying.ContinuousQuery;
import it.polimi.yasper.core.querying.ContinuousQueryExecution;
import it.polimi.yasper.core.sds.SDSConfiguration;
import it.polimi.yasper.core.stream.data.DataStreamImpl;
import sepses.streamVKG.stream.TcpSocketStream;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.jena.graph.Graph;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * Created by Kabul on 03/08/16.
 */
public class Main {

    static CSPARQLEngine sr;

    public static void main(String[] args) throws InterruptedException, IOException, ConfigurationException {


        String path = Main.class.getResource("/csparql.properties").getPath();
        SDSConfiguration config = new SDSConfiguration(path);
        EngineConfiguration ec = EngineConfiguration.loadConfig("/csparql.properties");

        ContinuousQuery q;
        ContinuousQueryExecution cqe;
        TcpSocketStream writer;
        DataStreamImpl<Graph> register;

        sr = new CSPARQLEngine(0, ec);


                System.out.println("WHO_LIKES_WHAT example");

                writer = new TcpSocketStream("Writer", "http://streamreasoning.org/csparql/streams/stream2", 6666);
                register = sr.register(writer);
                writer.setWritable(register);

        cqe = sr.register(getQuery("rtgp-q2", ".rspql"), config);
                q = cqe.getContinuousQuery();
                cqe.add(new GenericResponseSysOutFormatter("TABLE", true));




        System.out.println(q.toString());
        System.out.println("<<------>>");
        (new Thread(writer)).start();

    }

    public static String getQuery(String queryName, String suffix) throws IOException {
        URL resource = Main.class.getResource("/" + queryName + suffix);
        System.out.println(resource.getPath());
        File file = new File(resource.getPath());
        return FileUtils.readFileToString(file, StandardCharsets.UTF_8).replace("\r","");
    }

}
