package agent;

import java.io.*;
import java.util.*;

public class OutputManager {

    private static OutputManager instance = null;

    private Map<String,RowWriter> writers;

    private Class writerClass;
    private Filter filter;

    private OutputManager(String writerClassName, Filter filter) throws Exception {
        writers = new HashMap<String,RowWriter>();
        writerClass = Class.forName(writerClassName);
        this.filter = filter;
    }

    public static void init(String writerClassName, Filter filter) throws Exception {
        OutputManager.instance = new OutputManager(writerClassName, filter);
    }

    private RowWriter touchWriter(String table) throws Exception {
        RowWriter writer = null;
        synchronized(this) {
            if (!writers.containsKey(table)) {
                writer = (RowWriter)writerClass.newInstance();
                writer.init(table);
                writers.put(table, writer);
            } else {
                writer = writers.get(table);
            }
        }
        return writer;
    }

    private void closeAllI() throws Exception {
        synchronized(this) {
            for (Map.Entry<String, RowWriter> kv : writers.entrySet()) {
                RowWriter writer = kv.getValue();
                writer.close();
            }
        }
    }

    public static void write(RowBlock block) throws Exception {
        RowWriter writer = OutputManager.instance.touchWriter(block.table);
        synchronized(writer) {
            writer.write(block, OutputManager.instance.filter);
        }
    }

    public static void close(String table) throws Exception {
        RowWriter writer = OutputManager.instance.touchWriter(table);
        synchronized(writer) {
            writer.close();
        }
    }

    public static void closeAll() throws Exception {
        OutputManager.instance.closeAllI();
    }

}
