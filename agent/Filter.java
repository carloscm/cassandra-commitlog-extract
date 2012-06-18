package agent;

import java.util.*;

import org.apache.log4j.Logger;

// customise this class for your own input filtering

public class Filter {
	protected static Logger logger = Logger.getLogger(Filter.class.getName());


	public Filter(Map<String,Object> conf) throws Exception {
	}

	public boolean isValid(Row row) {
		return true;
	}

}
