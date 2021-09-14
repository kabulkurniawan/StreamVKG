package sepses.streamVKG;


import it.polimi.sr.rsp.csparql.engine.CSPARQLEngine;
import it.polimi.yasper.core.engine.config.EngineConfiguration;
import it.polimi.yasper.core.querying.ContinuousQueryExecution;
import it.polimi.yasper.core.sds.SDSConfiguration;
import it.polimi.yasper.core.stream.data.DataStreamImpl;
import org.apache.jena.query.ARQ;
import org.yaml.snakeyaml.constructor.Constructor;
import sepses.streamVKG.stream.StreamOutputFormatter;
import sepses.streamVKG.stream.TcpSocketStream;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.jena.graph.Graph;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Kabul on 24/07/2021.
 */
public class Main {

    public static <Int> void main(String[] args) throws InterruptedException, IOException, ConfigurationException {
        //get configuration
        Map<String, Object> s = readYamlFile("config.yaml");
        ArrayList<Integer> is= (ArrayList<Integer>) s.get("iStreams");
        String[] os = s.get("oStream").toString().split(":");
        System.out.println(os[1]);
        String queryDir = s.get("queryDir").toString();
        ArrayList<String> queryFiles = listFilesForFolder(new File(queryDir));
        String csparqlConf = s.get("csparqlConf").toString();



        ARQ.init();
        EngineConfiguration ec = new EngineConfiguration(csparqlConf);
        SDSConfiguration config = new SDSConfiguration(csparqlConf);
        PrintWriter wr = createTcpClient(os[0], Integer.parseInt(os[1]));

        //init engine for Query1
        CSPARQLEngine sr = new CSPARQLEngine(0, ec);

        for (int i=0; i<is.size();i++){
            //System.out.println(is.get(i));
            registerStream(sr, createTcpServer("http://example.org/stream"+i,is.get(i)));
        }
        for (int k=0;k<queryFiles.size();k++){
            //System.out.println(queryDir+queryFiles.get(k));

            registerQuery(sr, config, queryDir+queryFiles.get(k), ".rspql",wr);
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
        TcpSocketStream writer = new TcpSocketStream(stream, port);
        (new Thread(writer)).start();
        return writer;
    }

    public static Map<String, Object> readYamlFile(String file) throws FileNotFoundException{
        InputStream input = readFile(file);

        Yaml yaml = new Yaml();

        Map<String, Object> yamlContent = yaml.load(input);


        return yamlContent;

    }

    protected static InputStream readFile(String file) throws FileNotFoundException {
        final File initialFile = new File(file);
        //System.out.print(initialFile);
        final InputStream input = new FileInputStream(initialFile);
        return input;

    }


    public static ArrayList<String> listFilesForFolder(final File folder) {
        ArrayList<String> rulefiles = new ArrayList<String>();

        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                listFilesForFolder(fileEntry);
            } else {
                rulefiles.add(fileEntry.getName().replaceAll(".rspql",""));
                // System.out.println(fileEntry.getName());
            }
        }

        return rulefiles;
    }

}

