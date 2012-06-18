package agent;

import java.util.*;

public class IDBlockDumper implements RowGatherer {
    private CassandraReader cassandraReader;
    private String table;
    private int idBlockSize;

    public void init(String table) throws Exception {
        this.table = table;
        Map<String,Object> conf = ConfManager.getTable(table);
        int confBlockSize = ((Integer)conf.get("blockSize")).intValue();
        cassandraReader = new CassandraReader(table, null, confBlockSize + 1);
        String sIBS = ConfManager.getGlobal("idBlockSize");
        idBlockSize = Integer.parseInt(sIBS);
    }

    public void reset() throws Exception {
        cassandraReader.reset();
    }

    public RowBlock read() throws Exception {
        IDBlock idBlock = IDBlockManager.singleton().consumeBlock(table, idBlockSize);
        if (idBlock != null) {
            return cassandraReader.readMulti(idBlock.ids);
        }
        return new RowBlock(table);
    }

    public void close() throws Exception {
        // nothing to close, CassandraManager is shared and keeps open connections
    }

}
