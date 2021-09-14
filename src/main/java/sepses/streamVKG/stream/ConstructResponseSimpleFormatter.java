package sepses.streamVKG.stream;

import it.polimi.yasper.core.format.QueryResultFormatter;
import lombok.extern.log4j.Log4j;
import org.apache.jena.graph.Graph;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.ResourceFactory;

import java.io.ByteArrayOutputStream;
import java.util.Observable;

/**
 * Created by Kabul on 03/07/2017.
 */
@Log4j
public abstract class ConstructResponseSimpleFormatter extends QueryResultFormatter {

    long last_result = -1L;

    public ConstructResponseSimpleFormatter(String format, boolean distinct) {
        super(format, distinct);
    }

    @Override
    public void update(Observable o, Object arg) {
        Graph sr = (Graph) arg;
        this.format(sr);
    }

    public void format(Graph sr) {
        Model modelForGraph = ModelFactory.createModelForGraph(sr);
        Property p1=
                ResourceFactory.createProperty("http://streamreasoning.org/csparql/processingTime");
        Property p2=
                ResourceFactory.createProperty("http://streamreasoning.org/csparql/eventTime");

        modelForGraph.remove(null,p1,null);
        modelForGraph.remove(null,p2,null);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        modelForGraph.write(outputStream, format);
        log.debug("[" + System.currentTimeMillis() + "] Result at [" + last_result + "]");
        out(new String(outputStream.toByteArray()));
    }

    protected abstract void out(String s);
}