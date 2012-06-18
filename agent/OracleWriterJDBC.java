package agent;

import java.util.*;

public class OracleWriterJDBC extends OracleWriter implements RowWriter {

    private JDBCWriter writer;

    public void init(String table) throws Exception {
        this.table = table;
        writer = new JDBCWriter("oracle");
    }

    public void write(RowBlock block, Filter filter) throws Exception {
        writer.write(inserts(block, filter));
    }

    public void close() throws Exception {
        writer.close();
    }

}
