package agent;

import java.util.*;

public class IDBlock {
    public String table;
    public Set<String> ids;

    public IDBlock(String tableName) {
        table = tableName;
    	ids = new HashSet<String>();
    }

    public void append(IDBlock block) {
    	ids.addAll(block.ids);
    }

    public IDBlock consume(int count) {
    	IDBlock block = new IDBlock(table);
    	int i = 0;
    	for (String id : ids) {
    		if (count <= 0) {
    			break;
    		}
    		block.ids.add(id);
    		count--;
    	}
    	ids.removeAll(block.ids);
    	return block;
    }

}
