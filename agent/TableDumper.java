package agent;

import java.util.*;

public class TableDumper implements RowGatherer {
    private CassandraReader cassandraReader;

    public void init(String table) throws Exception {
        Map<String,Object> conf = ConfManager.getTable(table);
        int confBlockSize = ((Integer)conf.get("blockSize")).intValue();
        cassandraReader = new CassandraReader(table, null, confBlockSize + 1);
    }

    public void reset() throws Exception {
        cassandraReader.reset();
    }

    public RowBlock read() throws Exception {
        return cassandraReader.readSequential();
    }

    public void close() throws Exception {
        // nothing to close, CassandraManager is shared and keeps open connections
    }

}
