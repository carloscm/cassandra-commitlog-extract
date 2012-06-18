package agent;

import java.util.*;

import org.apache.log4j.Logger;

public class Flow implements Runnable {
	protected static Logger logger = Logger.getLogger(Flow.class.getName());

	private int lastBlockSize = 0;

	private List<String> tableNames;
	private String currentTableName;

	private Class readerClass;

	private boolean end;

	public Flow(List<String> tableNames, String readerClassName) throws Exception {
		this.tableNames = tableNames;
		readerClass = Class.forName(readerClassName);
		end = false;
	}

	public int checkAndClearLastBlockSize() {
		int r = 0;
		synchronized (this) {
			r = lastBlockSize;
			lastBlockSize = 0;
		}
		return r;
	}

	public String banner() {
		return currentTableName;
	}

	public boolean isFinished() {
		return end;
	}

	public void run() {

		try {

			for (String tableName : tableNames) {
				logger.info("Start: " + tableName);
				currentTableName = tableName;

				checkAndClearLastBlockSize();

        		RowGatherer reader = (RowGatherer)readerClass.newInstance();
        		reader.init(tableName);

		    	while (true) {
		    		RowBlock block = reader.read();
		    		if (block.rows.isEmpty()) {
		    			break;
		    		}
			    	OutputManager.write(block);
					synchronized (this) {
			    		lastBlockSize += block.rows.size();
			    	}
		    	}
	            reader.close();

				logger.info("End: " + tableName);
			}

	    } catch (Throwable t) {
	    	logger.error("Exception in flow:", t);

	    } finally {
	    	end = true;
	    }

	}

}
