package sepses.streamVKG;


import it.polimi.sr.rsp.csparql.engine.CSPARQLEngine;
import it.polimi.sr.rsp.csparql.sysout.GenericResponseSysOutFormatter;
//import it.polimi.sr.rsp.csparql.engine.CSPARQLEngine;
//import it.polimi.sr.rsp.csparql.sysout.GenericResponseSysOutFormatter;
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
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Created by Kabul on 24/07/2021.
 */
public class Main {

    static CSPARQLEngine sr;

    public static void main(String[] args) throws InterruptedException, IOException, ConfigurationException {

    	//target host
    	final String host1= "http://localhost:8080";
    	
    	 // examples name
        final int SINGLE_STREAM = 1;
        final int MULTI_STREAM = 2;
        
        // put here the example you want to run
        int key = SINGLE_STREAM;
    	
        String path = Main.class.getResource("/csparql.properties").getPath();
        SDSConfiguration config = new SDSConfiguration(path);
        EngineConfiguration ec = EngineConfiguration.loadConfig("/csparql.properties");

        ContinuousQuery q;
        ContinuousQueryExecution cqe;
        TcpSocketStream writer;
        DataStreamImpl<Graph> register;

        sr = new CSPARQLEngine(0, ec);

        switch (key) {
        case SINGLE_STREAM:
        	 System.out.println("WHO_LIKES_WHAT example");

             writer = new TcpSocketStream("Writer", "http://streamreasoning.org/csparql/streams/stream2", 7770);
             register = sr.register(writer);
             writer.setWritable(register);

             cqe = sr.register(getQuery("rtgp-q2", ".rspql"), config);
             q = cqe.getContinuousQuery();
             cqe.add(new GenericResponseSysOutFormatter("TABLE", true));
             String pq = preparseCsparqlQuery(q.toString());
		     System.out.println("<<------>>");
		      (new Thread(writer)).start();
             sendRequest(pq,host1);
		      break;
        
        case MULTI_STREAM:
	        	 writer = new TcpSocketStream("Writer", "http://streamreasoning.org/csparql/streams/stream2", 7770);
	             register = sr.register(writer);
	             writer.setWritable(register);
	             
	             TcpSocketStream writer2 = new TcpSocketStream("Writer", "http://streamreasoning.org/csparql/streams/stream3", 7771);
	             DataStreamImpl<Graph> register2 = sr.register(writer2);
	             writer2.setWritable(register2);

                TcpSocketStream writer3 = new TcpSocketStream("Writer", "http://streamreasoning.org/csparql/streams/stream4", 7772);
                DataStreamImpl<Graph> register3 = sr.register(writer3);
                writer3.setWritable(register3);
	
	             cqe = sr.register(getQuery("rtgp-q3", ".rspql"), config);
	             q = cqe.getContinuousQuery();
	             cqe.add(new GenericResponseSysOutFormatter("TABLE", true));

			        System.out.println(q.toString());
			        System.out.println("<<------>>");
			        (new Thread(writer)).start();
			        (new Thread(writer2)).start();
                    (new Thread(writer3)).start();
			        break;
			        
			        default:
			        	System.exit(0);
			        	break;
        }
        
    }

    private static String getQuery(String queryName, String suffix) throws IOException {
        URL resource = Main.class.getResource("/" + queryName + suffix);
        System.out.println(resource.getPath());
        File file = new File(resource.getPath());
        return FileUtils.readFileToString(file, StandardCharsets.UTF_8).replace("\r","");
    }
    
    private static void sendRequest(String sparql, String host) throws MalformedURLException, UnsupportedEncodingException {
    	URL url;
    	HttpURLConnection con;
        String param = URLEncoder.encode(sparql, StandardCharsets.UTF_8.toString());

		try {
			url = new URL(host+"/startservice?query="+param);
            System.out.println(url);
			con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");
            InputStream responseStream = con.getInputStream();
		} catch (Exception e) {
            e.printStackTrace();
        }
    	
    }
    private static String preparseCsparqlQuery(String q){
        String pq =q.replaceAll("<win","<http://win");
       return pq;
   }

}
