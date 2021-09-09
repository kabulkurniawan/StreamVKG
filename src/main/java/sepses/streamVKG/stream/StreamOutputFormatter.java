package sepses.streamVKG.stream;

import it.polimi.sr.rsp.csparql.sysout.ConstructResponseDefaultFormatter;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j;

import java.io.PrintWriter;
import java.net.Socket;


/**
 * Created by riccardo on 03/07/2017.
 */
@Log4j

public class StreamOutputFormatter extends ConstructResponseDefaultFormatter {

    public StreamOutputFormatter(String format, boolean distinct) {
        super(format, distinct);
    }

    @SneakyThrows
    @Override
    protected void out(String s) {

        Socket cs = new Socket("localhost",8880);

        PrintWriter writer = new PrintWriter(cs.getOutputStream(),true);
        writer.println(s);
    }
}