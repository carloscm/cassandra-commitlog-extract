package agent;

import java.util.*;

import org.apache.commons.lang.StringEscapeUtils;

public abstract class PostgresWriter {

    protected String table;
    protected boolean bulkMode;

    private void outStartBlock(StringBuffer sb) {
        if (bulkMode) {
            sb.append("COPY \"" + table + "\" FROM STDIN (DELIMITER '^');\n");
        } else {
            sb.append("WITH new_values (nid, nattributes) AS (VALUES\n");
        }
    }

    private void outNewRow(StringBuffer sb) {
        if (bulkMode) {
            sb.append("\n");
        } else {
            sb.append(",\n");
        }
    }

    private void outStartRow(StringBuffer sb, String key) {
        if (bulkMode) {
            sb.append(key);
            sb.append("^");
        } else {
            sb.append("('");
            sb.append(key);
            sb.append("','");
        }
    }

    private void outEndRow(StringBuffer sb, String value) {
        if (bulkMode) {
            sb.append(value);
        } else {
            sb.append(value);
            sb.append("'::hstore)");
        }
    }

    private void outEndBlock(StringBuffer sb) {
        if (bulkMode) {
            sb.append("\n\\.\n\n");
        } else {
            sb.append("),\n"+
                "upsert AS (\n"+
                "    UPDATE \"" + table + "\"\n"+
                "        SET id = nid,\n"+
                "            attributes = nattributes\n"+
                "    FROM new_values\n"+
                "    WHERE id = nid\n"+
                "    RETURNING \"" + table + "\".*\n"+
                ")\n"+
                "INSERT INTO \"" + table + "\" (id, attributes)\n"+
                "SELECT nid, nattributes\n"+
                "FROM new_values\n"+
                "WHERE NOT EXISTS (SELECT 1 \n"+
                "                  FROM upsert up \n"+
                "                  WHERE up.id = new_values.nid);\n\n");
        }
    }

    protected String inserts(RowBlock block, Filter filter) throws Exception {

        StringBuffer sb = new StringBuffer("");

        outStartBlock(sb);

		boolean firstRow = true;
        for (Row row : block.rows) {

        	String key = row.key;

            if (filter != null && !filter.isValid(row)) {
                continue;
            }

        	if (firstRow) {
        		firstRow = false;
        	} else {
                outNewRow(sb);
        	}

            if (key.length() > 90) {
                key = key.substring(0, 90);
            }
            outStartRow(sb, StringEscapeUtils.escapeSql(Utils.deFuxMore(Utils.deFux(key))));

			boolean first = true;

			StringBuffer sbr = new StringBuffer("");
	        for (Map.Entry<String, String> kv : row.columns.entrySet()) {
	            String value = kv.getValue();
	            if (value == null) {
	                continue;
	            }
	            String column = kv.getKey();

	            if (!first) {
	                sbr.append(",");
	            }
	            try {
	                escape(column, sbr);
	                sbr.append("=>");
	                escape(value, sbr);
	            } catch (Exception e) {
	                
	            }
	            first = false;
	        }

            outEndRow(sb, StringEscapeUtils.escapeSql(sbr.toString()));
        }

        outEndBlock(sb);

        return sb.toString();
    }

    private void escape(String v, StringBuffer sb) throws Exception {
        v = Utils.deFux(v);
        sb.append("\"");
        sb.append(Utils.deFuxMore(StringEscapeUtils.escapeJava(v)));
        sb.append("\"");
    }

}
