package agent;

import java.util.*;

public class RowBlock {
    public String table;
    public List<Row> rows;

    public RowBlock(String tableName) {
        table = tableName;
    	rows = new LinkedList<Row>();
    }

    // cache this as soon as possible, do not call on every block
    public Set<String> columnNames() {
    	Set<String> r = new HashSet<String>();
        for (Row row : rows) {
	        for (Map.Entry<String, String> kv : row.columns.entrySet()) {
				r.add(kv.getKey());
	        }
	    }
	    return r;
    }

}
