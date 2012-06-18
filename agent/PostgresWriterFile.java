package agent;

import java.util.*;

public class PostgresWriterFile extends PostgresWriter implements RowWriter {

    private FileWriter writer;

    public void init(String table) throws Exception {
        this.table = table;
        this.bulkMode = ConfManager.getGlobal("postgresBulkMode").equals("true");
        writer = new FileWriter(table, "pg.sql", 8*1024*1024);
    }

    public void write(RowBlock block, Filter filter) throws Exception {
        writer.write(inserts(block, filter));
    }

    public void close() throws Exception {
        writer.close();
    }

}
