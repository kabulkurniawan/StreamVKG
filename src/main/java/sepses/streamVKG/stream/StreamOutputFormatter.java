package sepses.streamVKG.stream;

import it.polimi.sr.rsp.csparql.sysout.ConstructResponseDefaultFormatter;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;


/**
 * Created by riccardo on 03/07/2017.
 */
@Log4j

public class StreamOutputFormatter extends ConstructResponseDefaultFormatter {
    protected PrintWriter writer;

    public StreamOutputFormatter(String format, boolean distinct) throws IOException {

        super(format, distinct);
        Socket cs = new Socket("localhost",8880);
        writer = new PrintWriter(cs.getOutputStream(),true);
    }

    @SneakyThrows
    @Override
    protected void out(String s) {
        writer.println(s);
    }
}