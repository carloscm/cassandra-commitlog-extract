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
import me.prettyprint.hector.api.ddl.*;

import org.apache.cassandra.thrift.*;

import org.apache.commons.lang.StringEscapeUtils;

import java.io.*;
import java.util.*;
import java.lang.reflect.Method;

// connection management for cassandra
public class CassandraManager {

    private String keyspaceName;
    private Keyspace keyspace = null;
    private Cluster cluster = null;
    
    public CassandraManager() throws Exception {

        Map<String,Object> conf = ConfManager.getConnection("cassandra");

        @SuppressWarnings("unchecked")
        List<String> hosts = (List<String>)conf.get("hosts");
        String clusterName = (String)conf.get("cluster");
        keyspaceName = (String)conf.get("keyspace");

        boolean first = true;
        for (String host : hosts) {
            if (first) {
                CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator(host);
                cluster = HFactory.getOrCreateCluster(clusterName, cassandraHostConfigurator);
                first = false;
            } else {
                cluster.addHost(new CassandraHost(host), false);
            }
        }

        ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();

        String rc = (String)conf.get("readConsistency");
        String wc = (String)conf.get("writeConsistency");
        if (rc.equals("ALL")) {
            ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ALL);
        } else if (rc.equals("QUORUM")) {
            ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.QUORUM);
        } else {
            ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
        }
        if (rc.equals("ALL")) {
            ccl.setDefaultWriteConsistencyLevel(HConsistencyLevel.ALL);
        } else if (rc.equals("QUORUM")) {
            ccl.setDefaultWriteConsistencyLevel(HConsistencyLevel.QUORUM);
        } else {
            ccl.setDefaultWriteConsistencyLevel(HConsistencyLevel.ONE);
        }
        keyspace = HFactory.createKeyspace(keyspaceName, cluster, ccl);
    }

    public Keyspace getKeyspace() {
        return keyspace;
    }

    public KeyspaceDefinition getKeyspaceDefinition() {
        return cluster.describeKeyspace(keyspaceName) ;
    }
    
    public void close() throws Exception {
    	cluster.getConnectionManager().shutdown();
    }

    private static CassandraManager instance = null;

    public static CassandraManager singleton() throws Exception {
        synchronized(CassandraManager.class) {
            if (CassandraManager.instance == null) {
                CassandraManager.instance = new CassandraManager();
            }
        }
        return CassandraManager.instance;
    }

}
