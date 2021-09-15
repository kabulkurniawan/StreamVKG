package sepses.streamVKG.stream;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j;

import java.io.IOException;
import java.io.PrintWriter;


/**
 * Created by riccardo on 03/07/2017.
 */
@Log4j

public class StreamOutputFormatter extends ConstructResponseSimpleFormatter {
    protected PrintWriter writer;

    public StreamOutputFormatter(String format, boolean distinct, PrintWriter wr) throws IOException {

        super(format, distinct);
        writer = wr;

    }

    @SneakyThrows
    protected void out(String s) {
        writer.println(s+"\\n");
    }
}