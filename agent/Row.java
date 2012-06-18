package agent;

import java.util.*;

public class Row {
    public String key;
    public Map<String,String> columns;

    public Row(String key) {
    	this.key = key;
        columns = new HashMap<String,String>();
    }
}
