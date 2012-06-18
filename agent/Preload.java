package agent;

import java.io.*;
import java.util.*;

import org.apache.log4j.Logger;

// preload some useful ID-to-website mappings. fill this with data reads for Filter operations, for example

public class Preload {
    protected static Logger logger = Logger.getLogger(Preload.class.getName());

    private static Preload instance = null;

	public Preload() throws Exception {
	}

    public static void init() throws Exception {
        Preload.instance = new Preload();
    }
}
