package agent;

import java.util.*;

import org.apache.commons.lang.StringEscapeUtils;

// follows http://tools.ietf.org/html/rfc4180

public class CSVWriter implements RowWriter {

    private FileWriter writer;
    private ArrayList<String> columns;
    private boolean outputHeader;
    private String path;

    public void init(String table) throws Exception {

        Map<String,Object> conf = ConfManager.getConnection("csv");

        outputHeader = true;
        if (conf.containsKey("header")) {
            outputHeader = ((Boolean)conf.get("header")).booleanValue();
        }

        long fileSize = 8*1024*1024;
        if (conf.containsKey("fileSize")) {
            try {
                fileSize = ((Long)conf.get("fileSize")).longValue();
            } catch (ClassCastException e){
                fileSize = ((Integer)conf.get("fileSize")).intValue();
            }
        }

        if (conf.containsKey("path")) {
            this.path = (String)conf.get("path");
        } else {
            this.path = "./";
        }

        this.columns = null;

        Map<String,Object> ct = (Map<String,Object>)ConfManager.getTable(table);
        boolean printColumnsNow = false;

        if (ct.containsKey("fixedColumns")) {
            LinkedHashMap<String,String> cols = (LinkedHashMap<String,String>)ct.get("fixedColumns");
            this.columns = new ArrayList<String>();
            this.columns.add("id");
            for (Map.Entry<String, String> kv : cols.entrySet()) {
                this.columns.add(kv.getKey());
            }
            printColumnsNow = true;
        }

        writer = new FileWriter(this.path + table, "csv", fileSize);
        if (printColumnsNow) {
            writeColumns();
        }
    }

    private void writeColumns() throws Exception {
        boolean first = true;
        StringBuilder sb = new StringBuilder("");
        for (String column : columns) {
            if (!first) {
                sb.append(",");
            } else {
                first = false;
            }
            sb.append(column);
        }
        sb.append("\n");
        writer.write(sb.toString());
    }

    public void write(RowBlock block, Filter filter) throws Exception {

        StringBuffer sb = new StringBuffer("");

        for (Row row : block.rows) {

            if (columns == null) {
                Set<String> sc = block.columnNames();
                sc.remove("id");
                columns = new ArrayList<String>(sc);
                columns.add(0, "id");
                writeColumns();
            }

            if (filter != null && !filter.isValid(row)) {
                continue;
            }

            boolean first = true;
            for (String column : columns) {
                if (!first) {
                    sb.append(",");
                } else {
                    first = false;
                }
                String value = row.columns.get(column);
                if (value == null) {
                    //sb.append(
                } else {
                    escape(value, sb);
                }
            }

            sb.append("\n");
        }

        writer.write(sb.toString());
    }

    public void close() throws Exception {
        writer.close();
    }

    private void escape(String v, StringBuffer sb) throws Exception {
        v = Utils.deFuxMore(v);
        v = Utils.fixEncoding(v);
        v = StringEscapeUtils.escapeCsv(v);
        sb.append(v);
    }

}
