package agent;

// https://github.com/rantav/hector
import me.prettyprint.cassandra.serializers.*;
import me.prettyprint.cassandra.model.*;
import me.prettyprint.hector.api.*;
import me.prettyprint.cassandra.service.*;
import me.prettyprint.hector.api.beans.*;
import me.prettyprint.hector.api.factory.*;
import me.prettyprint.hector.api.query.*;
import me.prettyprint.hector.api.exceptions.*;

import org.apache.cassandra.thrift.*;

import org.apache.commons.lang.StringEscapeUtils;

import java.io.*;
import java.util.*;
import java.lang.reflect.Method;

// multi purpose reader class used by all cassandra readers: dumpers, ID gatherers, preloaders
public class CassandraReader {

	private String table;
    private String[] columns;
	private String lastKey;
	private int blockSize;
	private CassandraManager manager;
    private RangeSlicesQuery<String, String, String> rangeSlicesQuery;
    private MultigetSliceQuery<String, String, String> multigetSliceQuery;

	public CassandraReader(String table, String[] columns, int blockSize) throws Exception {
		this.manager = CassandraManager.singleton();
        this.table = table;
        this.columns = columns;
        this.blockSize = blockSize;
        reset();
	}

    public void reset() throws Exception {
    	lastKey = "";
        StringSerializer stringSerializer = StringSerializer.get(); 
        Keyspace keyspace = manager.getKeyspace();

        rangeSlicesQuery = HFactory.createRangeSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);            
        rangeSlicesQuery.setColumnFamily(table);
        rangeSlicesQuery.setKeys(lastKey, "");
        rangeSlicesQuery.setRowCount(blockSize);
        if (columns != null) {
            rangeSlicesQuery.setColumnNames(columns);
        } else {
            rangeSlicesQuery.setRange("", "", false, 100000);
        }

        multigetSliceQuery = HFactory.createMultigetSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);            
        multigetSliceQuery.setColumnFamily(table);
        //multigetSliceQuery.setKeys(
        if (columns != null) {
            multigetSliceQuery.setColumnNames(columns);
        } else {
            multigetSliceQuery.setRange("", "", false, 100000);
        }
    }

    private RowBlock processResult(Rows<String, String, String> orderedRows) {
        RowBlock block = new RowBlock(table);

        for (me.prettyprint.hector.api.beans.Row<String, String, String> cassandraRow : orderedRows) {
            String key = cassandraRow.getKey();
            // "" will never be a valid ID in our model, so this is safe for the first iteration too
            if (key.equals(lastKey)) {
                continue;
            }
            lastKey = key;

            agent.Row row = new agent.Row(key);

            List columns = cassandraRow.getColumnSlice().getColumns();
            
            int n = 0;
            for (Iterator iterator = columns.iterator(); iterator.hasNext();) {
                HColumn column = (HColumn)iterator.next();
                row.columns.put(column.getName().toString(), column.getValue().toString());
                n++;
            }
            // skip empty rows
            if (n == 0) {
                continue;
            }

            block.rows.add(row);
        }

        return block;
    }

    public RowBlock readMulti(Iterable<String> i) throws Exception {
        multigetSliceQuery.setKeys(i);
        Rows<String, String, String> orderedRows = multigetSliceQuery.execute().get();
        return processResult(orderedRows);
    }

    public RowBlock readSequential() throws Exception {
        rangeSlicesQuery.setKeys(lastKey, "");
		OrderedRows<String, String, String> orderedRows = rangeSlicesQuery.execute().get();
        return processResult(orderedRows);
    }

}
