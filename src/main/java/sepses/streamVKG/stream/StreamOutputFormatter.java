package sepses.streamVKG.stream;

import it.polimi.sr.rsp.csparql.sysout.ConstructResponseDefaultFormatter;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j;
import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;


/**
 * Created by riccardo on 03/07/2017.
 */
@Log4j

public class StreamOutputFormatter extends ConstructResponseDefaultFormatter {
    protected PrintWriter writer;

    public StreamOutputFormatter(String format, boolean distinct, PrintWriter wr) throws IOException {

        super(format, distinct);
        writer = wr;

    }

    @SneakyThrows
    protected void out(String s) {
        Model model = ModelFactory.createDefaultModel()
                .read(IOUtils.toInputStream(s, "UTF-8"), null, "TURTLE");
        Property p1 = model.createProperty("http://streamreasoning.org/csparql/eventTime");
        Property p2 = model.createProperty("http://streamreasoning.org/csparql/processingTime");
        model.remove(null,p1,null);
        model.remove(null,p2,null);
        writer.println(model.write(System.out,"TURTLE"));
        model.close();
    }
}