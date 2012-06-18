package agent;

import java.io.*;
import java.util.*;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.log4j.Logger;

public class ConfManager {

    protected static Logger logger = Logger.getLogger(Main.class.getName());

    private static Map<String,String> confGlobals;
    @SuppressWarnings("unchecked")
    private static Map<String,Object> confConnections;
    @SuppressWarnings("unchecked")
    private static Map<String,Object> confTables;
    @SuppressWarnings("unchecked")
    private static Map<String,Object> confExporters;

    public static void init(String confPath) throws Exception {
        confGlobals = new HashMap<String,String>();

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

        confConnections = mapper.readValue(new File(confPath + "/connections.conf"), Map.class);
        logger.info(confPath + "/connections.conf OK");
        confTables = mapper.readValue(new File(confPath + "/tables.conf"), Map.class);
        logger.info(confPath + "/tables.conf OK");
        confExporters = mapper.readValue(new File(confPath + "/exporters.conf"), Map.class);
        logger.info(confPath + "/exporters.conf OK");
    }

    public static void setGlobal(String name, String value) {
        confGlobals.put(name, value);
    }

    public static String getGlobal(String name) {
        return confGlobals.get(name);
    }

    public static Map<String,Object> getConnection(String name) {
        @SuppressWarnings("unchecked")
        Map<String,Object> r;
        synchronized(ConfManager.class) {
            r = (Map<String,Object>)confConnections.get(name);
        }
        return r;
    }

    public static Map<String,Object> getTable(String name) {
        Map<String,Object> r;
        @SuppressWarnings("unchecked")
        Map<String,Object> defaultTable;
        @SuppressWarnings("unchecked")
        Map<String,Object> table;
        synchronized(ConfManager.class) {
            defaultTable = (Map<String,Object>)confTables.get("*default");
            table = (Map<String,Object>)confTables.get(name);
        }
        r = new HashMap<String,Object>();
        if (defaultTable != null) {
            r.putAll(defaultTable);
        }
        if (table != null) {
            r.putAll(table);
        }
        return r;
    }

    public static Map<String,Object> getExporter(String name) {
        @SuppressWarnings("unchecked")
        Map<String,Object> r;
        synchronized(ConfManager.class) {
            r = (Map<String,Object>)confExporters.get(name);
        }
        return r;
    }
}
