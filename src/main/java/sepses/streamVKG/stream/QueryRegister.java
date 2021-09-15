package sepses.streamVKG.stream;

import it.polimi.sr.rsp.csparql.engine.CSPARQLEngine;
import it.polimi.yasper.core.engine.config.EngineConfiguration;
import it.polimi.yasper.core.querying.ContinuousQueryExecution;
import it.polimi.yasper.core.sds.SDSConfiguration;
import it.polimi.yasper.core.stream.data.DataStreamImpl;
import org.apache.commons.io.FileUtils;
import org.apache.jena.graph.Graph;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class QueryRegister  implements Runnable  {
    private CSPARQLEngine sr;
    private SDSConfiguration config;
    private String queryName;
    private String suffix;
    private PrintWriter wr;
    private ArrayList<TcpSocketStream> arrWrs;

    public QueryRegister(ArrayList<TcpSocketStream> wrs, EngineConfiguration ec, SDSConfiguration cf, String qn, String sf, PrintWriter w) {
         sr = new CSPARQLEngine(0, ec);
         config = cf;
         queryName = qn;
         suffix = sf;
         wr = w;
         arrWrs = wrs;
    }


    public void run() {

        try {
            for(int n=0;n<arrWrs.size();n++){
                registerStream(sr,arrWrs.get(n));
            }

            ContinuousQueryExecution cqe = sr.register(getQuery(queryName, suffix), config);
            cqe.add(new StreamOutputFormatter("TURTLE", true,wr));
        } catch (Exception e) {
            e.printStackTrace();
        }
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

}
