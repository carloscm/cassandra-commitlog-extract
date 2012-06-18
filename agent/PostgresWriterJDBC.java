package agent;

import java.util.*;

public class PostgresWriterJDBC extends PostgresWriter implements RowWriter {

    private JDBCWriter writer;

    public void init(String table) throws Exception {
        this.table = table;
        writer = new JDBCWriter("postgres");
    }

    public void write(RowBlock block, Filter filter) throws Exception {
        List<String> l = new LinkedList<String>();
        l.add(inserts(block, filter));
        writer.write(l);
    }

    public void close() throws Exception {
        writer.close();
    }

}
