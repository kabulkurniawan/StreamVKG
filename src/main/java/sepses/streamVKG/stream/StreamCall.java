package sepses.streamVKG.stream;
import lombok.SneakyThrows;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class StreamCall  implements Runnable  {
    private String sparql;
    private String host;

    public StreamCall(String q, String host) {
        this.sparql = preparseCsparqlQuery(q.toString());
        this.host = host;

    }

    @SneakyThrows
    public void run() {


        URL url;
        HttpURLConnection con;
        String param = URLEncoder.encode(sparql, StandardCharsets.UTF_8.toString());

        try {
            url = new URL(host+"/startservice?query="+param);
            System.out.println(url);
            con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            con.getInputStream();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String preparseCsparqlQuery(String q){
        String pq =q.replaceAll("<win","<http://win");
        return pq;
    }

}
