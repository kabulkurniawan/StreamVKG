package sepses.streamVKG.stream;

import it.polimi.sr.rsp.csparql.sysout.ConstructResponseDefaultFormatter;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j;
import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

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
        System.out.println("output: =>"+s);
        writer.println(s);
    }
}