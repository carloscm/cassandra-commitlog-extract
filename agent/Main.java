package agent;

import java.util.*;

import org.apache.log4j.Logger;

public class Main {
	protected static Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {

        logger.info("Loading config files...");
        ConfManager.init(args[0]);

        //JDBCManager.singleton();

        String exporterName = args[1];
        logger.info("Launching exporter '" + exporterName + "'");

        Map<String,Object> exporterConf = ConfManager.getExporter(exporterName);

        if (exporterConf.containsKey("initPreload") && ((Boolean)exporterConf.get("initPreload")).booleanValue()) {
            logger.info("Preloading some ID mappings from Cassandra...");
            Preload.init();
        }

        if (exporterConf.containsKey("initCommitLogReader") && ((Boolean)exporterConf.get("initCommitLogReader")).booleanValue()) {
            String commitLogFolder = (String)exporterConf.get("commitLogFolder");
            int binSize = ((Integer)exporterConf.get("commitLogBinSize")).intValue();
            Object periodO = exporterConf.get("commitLogPeriod");
            long period = (long)(((Integer)periodO).intValue());
            int idBlockSize = ((Integer)exporterConf.get("idBlockSize")).intValue();
            ConfManager.setGlobal("idBlockSize", ""+idBlockSize);
            logger.info("Reading commit logs from "+commitLogFolder+", bin size " + binSize +
                ", id block size " + idBlockSize + ", period " + period + " seconds");
            CommitLogReader r = new CommitLogReader(binSize);
            r.readCommitLogDirectory(commitLogFolder, period);
        }

        if (exporterConf.containsKey("postgresBulkMode") && ((Boolean)exporterConf.get("postgresBulkMode")).booleanValue()) {
            ConfManager.setGlobal("postgresBulkMode", "true");
        } else {
            ConfManager.setGlobal("postgresBulkMode", "false");
        }

        List<Flow> flows = new LinkedList<Flow>();
        List<Thread> running = new LinkedList<Thread>();

        String readerClassName = (String)exporterConf.get("reader");
        String writerClassName = (String)exporterConf.get("writer");

        Map<String,Object> filterConf = (Map<String,Object>)exporterConf.get("filter");
        Filter filter = null;
        if (filterConf != null) {
            filter = new Filter(filterConf);
        }

        List<List<String>> flowDefinitions = (List<List<String>>)exporterConf.get("flows");

        // output is shared and multithreaded, init the output manager
        OutputManager.init(writerClassName, filter);

        // create a flow for every table listing inside the flow definitions, and launch them
        for (List<String> flowTables : flowDefinitions) {
            Flow flow = new Flow(flowTables, readerClassName);
            Thread t = new Thread(flow);
            t.start();
            running.add(t);
            flows.add(flow);
        }

        // monitor the threads and wait for them to die
        Monitor m = new Monitor(flows);
        for (Thread t : running) {
            t.join(0);
        }

        OutputManager.closeAll();

        m.end();
    }
}
