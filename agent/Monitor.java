package agent;

import java.util.*;

import org.apache.log4j.Logger;

public class Monitor implements Runnable {
	protected static Logger logger = Logger.getLogger(Monitor.class.getName());

    private List<Flow> flows;
    private Thread t;
    private boolean endNow;

    public Monitor(List<Flow> flows) throws Exception {
        this.flows = flows;
        t = new Thread(this);
        endNow = false;
        t.start();
    }

    public void run() {
        try {
            while (!endNow) {
                Thread.sleep(1000);
                int processed = 0;
                String tables = "";
                for (Flow flow : flows) {
                    if (flow.isFinished()) {
                        continue;
                    }
                    int flowP = flow.checkAndClearLastBlockSize();
                    tables = tables + flow.banner() + ": " + flowP + "    ";
                    processed += flowP;
                }
                logger.info("Last 1s: " + processed + " rows       / " + tables);
            }
        } catch (Throwable t) {
            logger.error("Exception in monitor:", t);
        }
    }

    public void end() {
        endNow = true;
    }
}
