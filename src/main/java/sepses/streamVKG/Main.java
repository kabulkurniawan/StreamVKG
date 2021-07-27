package sepses.streamVKG;


import it.polimi.sr.rsp.csparql.engine.CSPARQLEngine;
import it.polimi.sr.rsp.csparql.sysout.GenericResponseSysOutFormatter;
import it.polimi.yasper.core.engine.config.EngineConfiguration;
import it.polimi.yasper.core.querying.ContinuousQuery;
import it.polimi.yasper.core.querying.ContinuousQueryExecution;
import it.polimi.yasper.core.sds.SDSConfiguration;
import it.polimi.yasper.core.stream.data.DataStreamImpl;
import sepses.streamVKG.stream.StreamCall;
import sepses.streamVKG.stream.TcpSocketStream;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.jena.graph.Graph;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * Created by Kabul on 24/07/2021.
 */
public class Main {

    static CSPARQLEngine sr;

    public static void main(String[] args) throws InterruptedException, IOException, ConfigurationException {

    	//target host
    	final String host1= "http://localhost:8080";
        final String host2= "http://10.10.20.5:8080";
        final String host3= "http://10.10.30.8:8080";
        //final String host4= "http://10.10.30.7:8080";
    	
    	 // examples name
        final int SINGLE_STREAM = 1;
        final int MULTI_STREAM = 2;
        
        // put here the example you want to run
        int key = MULTI_STREAM;
    	
        String path = Main.class.getResource("/csparql.properties").getPath();
        SDSConfiguration config = new SDSConfiguration(path);
        EngineConfiguration ec = EngineConfiguration.loadConfig("/csparql.properties");

        ContinuousQuery q;
        ContinuousQueryExecution cqe;
        TcpSocketStream writer;
        StreamCall sc;
        DataStreamImpl<Graph> register;

        sr = new CSPARQLEngine(0, ec);

        switch (key) {
        case SINGLE_STREAM:
                System.out.println("SINGLE STREAM");

                writer = new TcpSocketStream("Writer", "http://streamreasoning.org/csparql/streams/stream2", 7770);
                register = sr.register(writer);
                writer.setWritable(register);

                cqe = sr.register(getQuery("rtgp-q2", ".rspql"), config);
                q = cqe.getContinuousQuery();
                cqe.add(new GenericResponseSysOutFormatter("TABLE", true));

                //call host
                sc = new StreamCall(q.toString(),host1);
		        System.out.println("<<------>>");
		        (new Thread(writer)).start();
                (new Thread(sc)).start();

		        break;
        
        case MULTI_STREAM:

                System.out.println("MULTI STREAM");
                //host1
	        	 writer = new TcpSocketStream("Writer", "http://streamreasoning.org/csparql/streams/stream2", 7770);
	             register = sr.register(writer);
	             writer.setWritable(register);

	            //host2
	             TcpSocketStream writer2 = new TcpSocketStream("Writer", "http://streamreasoning.org/csparql/streams/stream3", 7771);
	             DataStreamImpl<Graph> register2 = sr.register(writer2);
	             writer2.setWritable(register2);

	             //host3
                TcpSocketStream writer3 = new TcpSocketStream("Writer", "http://streamreasoning.org/csparql/streams/stream4", 7772);
                DataStreamImpl<Graph> register3 = sr.register(writer3);
                writer3.setWritable(register3);

                //host4
                //TcpSocketStream writer4 = new TcpSocketStream("Writer", "http://streamreasoning.org/csparql/streams/stream5", 7773);
                //DataStreamImpl<Graph> register4 = sr.register(writer4);
                //writer3.setWritable(register4);

	             cqe = sr.register(getQuery("rtgp-q3", ".rspql"), config);
	             q = cqe.getContinuousQuery();
	             cqe.add(new GenericResponseSysOutFormatter("TABLE", true));

                //call host

                sc = new StreamCall(q.toString(),host1);
                StreamCall sc2 = new StreamCall(q.toString(),host2);
                StreamCall sc3 = new StreamCall(q.toString(),host3);
                //StreamCall sc4 = new StreamCall(q.toString(),host4);


                    System.out.println("<<------>>");
                    (new Thread(writer)).start();
                    (new Thread(sc)).start();
			        (new Thread(writer2)).start();
                    (new Thread(sc2)).start();
                    (new Thread(writer3)).start();
                    (new Thread(sc3)).start();
                    //(new Thread(writer4)).start();
                    //(new Thread(sc4)).start();


            break;
			        
			        default:
			        	System.exit(0);
			        	break;
        }
        
    }

    public static String getQuery(String queryName, String suffix) throws IOException {
        URL resource = Main.class.getResource("/" + queryName + suffix);
        System.out.println(resource.getPath());
        File file = new File(resource.getPath());
        return FileUtils.readFileToString(file, StandardCharsets.UTF_8).replace("\r","");
    }



}
