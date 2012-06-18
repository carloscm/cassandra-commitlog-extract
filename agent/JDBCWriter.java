package agent;

import java.io.*;
import java.util.*;
import java.sql.*;

import org.apache.log4j.Logger;

public class JDBCWriter {
    protected static Logger logger = Logger.getLogger(JDBCWriter.class.getName());

    private Connection conn;
    private Statement stmt;
    private String url;

    public JDBCWriter(String config) throws Exception {
        Map<String,Object> conf = ConfManager.getConnection(config);
        url = (String)conf.get("url");
        String driver = (String)conf.get("driver");
        String user = (String)conf.get("user");
        String password = (String)conf.get("password");

        logger.info("Opening new connection to " + url + "...");

        Class.forName(driver);
        conn = DriverManager.getConnection(url, user, password);
        //conn.setAutoCommit(false);
        stmt = conn.createStatement();
    }

    public void close() throws Exception {
        logger.info("Closing connection to " + url + "...");

        if (stmt != null)
            stmt.close();
        stmt = null;
        if (conn != null)
            conn.close();
        conn = null;
    }

    public void write(List<String> l) throws Exception {
        stmt.clearBatch();
        for (String s : l) {
            stmt.addBatch(s);
        }
        stmt.executeBatch();
    }

}
