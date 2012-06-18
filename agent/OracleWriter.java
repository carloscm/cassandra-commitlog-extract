package agent;

import java.util.*;

import org.apache.commons.lang.StringEscapeUtils;

public abstract class OracleWriter {

    protected String table;

    protected List<String> inserts(RowBlock block, Filter filter) throws Exception {

        Map<String,Object> ct = (Map<String,Object>)ConfManager.getTable(table);
        Map<String,String> cols = (Map<String,String>)ct.get("fixedColumns");

        List<String> r = new LinkedList<String>();

        for (Row row : block.rows) {
            StringBuilder sb = new StringBuilder("");

        	String key = row.key;

            if (filter != null && !filter.isValid(row)) {
                continue;
            }

            if (key.length() > 90) {
                key = key.substring(0, 90);
            }

            sb.append("MERGE INTO \"" + table + "\" USING dual ON (\"id\"=");
            escape(key, sb);
            sb.append(")\nWHEN NOT MATCHED THEN INSERT (\"id\"");
            StringBuilder sbd = new StringBuilder("");
            escape(key, sbd);

            for (Map.Entry<String, String> kv : cols.entrySet()) {
                sb.append(",");
                sbd.append(",");

                String name = kv.getKey();
                sb.append("\""+ name +"\"");

                String value = row.columns.get(name);
                if (value == null) {
                    value = "";
                }
                escape(value, sbd);
            }
            sb.append(") VALUES (");
            sb.append(sbd.toString());
            sb.append(")\nWHEN MATCHED THEN UPDATE SET ");

            boolean first = true;
            for (Map.Entry<String, String> kv : cols.entrySet()) {
                if (first) {
                    first = false;
                } else {
                    sb.append(",");
                }
                String name = kv.getKey();
                sb.append("\""+ name +"\"=");

                String value = row.columns.get(name);
                if (value == null) {
                    value = "";
                }
                escape(value, sb);
            }
            r.add(sb.toString());
        }

        return r;
    }

    private void escape(String v, StringBuilder sb) throws Exception {
        if (v.length() > 2900) {
            v = v.substring(0, 2900);
        }
        v = Utils.deFux(v);
        sb.append("'");
        sb.append(Utils.deFuxMore(StringEscapeUtils.escapeSql(v)));
        sb.append("'");
    }

}
