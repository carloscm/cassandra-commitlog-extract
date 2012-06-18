package agent;

import java.util.*;

public class OracleWriterFile extends OracleWriter implements RowWriter {

    private FileWriter writer;

    public void init(String table) throws Exception {
        this.table = table;
        //this.bulkMode = ConfManager.getGlobal("postgresBulkMode").equals("true");
        writer = new FileWriter(table, "oracle.sql", 8*1024*1024);
    }

    public void write(RowBlock block, Filter filter) throws Exception {
        StringBuilder sb = new StringBuilder("");
        List<String> l = inserts(block, filter);
        for (String s : l) {
            sb.append(s);
        }
        writer.write(sb.toString());
    }

    public void close() throws Exception {
        writer.close();
    }

}
