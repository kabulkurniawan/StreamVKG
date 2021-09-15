package sepses.streamVKG;


import it.polimi.yasper.core.engine.config.EngineConfiguration;
import it.polimi.yasper.core.sds.SDSConfiguration;
import org.apache.jena.query.ARQ;
import sepses.streamVKG.stream.QueryRegister;
import sepses.streamVKG.stream.TcpSocketStream;
import org.apache.commons.configuration.ConfigurationException;
import org.yaml.snakeyaml.Yaml;
import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by Kabul on 24/07/2021.
 */
public class Main {

    public static <Int> void main(String[] args) throws InterruptedException, IOException, ConfigurationException {
        //get configuration file
        Map<String, Object> s = readYamlFile("config.yaml");
        ArrayList<Integer> is= (ArrayList<Integer>) s.get("iStreams");
        String[] os = s.get("oStream").toString().split(":");
        String queryDir = s.get("queryDir").toString();
        ArrayList<String> queryFiles = listFilesForFolder(new File(queryDir));
        String csparqlConf = s.get("csparqlConf").toString();


        //init configuration
        ARQ.init();
        EngineConfiguration ec = new EngineConfiguration(csparqlConf);
        SDSConfiguration config = new SDSConfiguration(csparqlConf);
        PrintWriter wr = createTcpClient(os[0], Integer.parseInt(os[1]));



        //create TCP Server
        ArrayList<TcpSocketStream> arrWrs = new ArrayList<TcpSocketStream>();
        for (int i=0; i<is.size();i++) {
           TcpSocketStream wrs = createTcpServer("http://streamreasoning.org/csparql/streams/stream"+i,is.get(i));
            arrWrs.add(wrs);
        }

        //create engine per query
       for (int k=0;k<queryFiles.size();k++){
           QueryRegister qr = new QueryRegister(arrWrs, ec, config, queryDir + queryFiles.get(k), ".rspql", wr);
           (new Thread(qr)).start();
       }



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
            }
        }
        return rulefiles;
    }



}

