package account;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Table entity in database
 */
public class Table {
    private String tableName = null;
    private Map<String, String> columns = new HashMap<String, String>();
    private List<Map<String, Object>> values;

    public Table(String tableName) {
        this.tableName = tableName;
        this.values = new ArrayList<Map<String, Object>>();
    }

    /**
     * @return all the columns in the table
     */
    public Map<String, String> getColumns() {
        return this.columns;
    }

    /**
     * inserts new record in the table
     *
     * @param row
     * @throws Exception
     */
    public void addRecord(Map<String, Object> row) throws Exception {
        Map<String, Object> insertRow = new HashMap<String, Object>();
        for (String column : row.keySet()) {
            if (isColumn(column)) {
                String type = columns.get(column);
                insertRow.put(column, typeCast((String) row.get(column), type));
            } else {
                System.out.println("Insert Failed");
                throw new Exception("Insert Failed");
            }
        }
        values.add(insertRow);
    }

    /**
     * function for type casting the data types
     *
     * @param value
     * @param type
     * @return
     * @throws Exception
     */
    private Object typeCast(String value, String type) throws Exception {
        switch (type.toUpperCase()) {
            case "INT":
            case "TINYINT":
            case "INTEGER":
            case "SMALLINT":
                return Integer.parseInt(value);
            case "BIGINT":
                return Long.parseLong(value);
            case "FLOAT":
            case "DOUBLE":
            case "DECIMAL":
                return Double.parseDouble(value);
            case "DATE":
            case "TIME":
            case "DATETIME":
            case "TIMESTAMP":
                return java.sql.Date.valueOf(value);
            case "CHAR":
            case "TEXT":
            case "VARCHAR":
            case "LONGTEXT":
                return value;
            default:
                throw new IllegalArgumentException("Unsupported data type: " + type);
        }
    }

    /**
     * Sets columns
     *
     * @param columns
     */
    public void setColumns(Map<String, String> columns) {
        this.columns = columns;
    }

    /**
     * deletes rows in a table
     *
     * @param whereClause
     */
    public void deleteRecord(String whereClause) {
        String[] condition = whereClause.split("=");
        String col = condition[0].trim();
        String val = condition[1].trim();
        for (Map<String, Object> row : this.values) {
            if (row.containsKey(col) && row.get(col).toString().equals(val)) {
                row.remove(col);
            }
        }
    }

    /**
     * @param columnName
     * @return true if the column exists.
     */
    public boolean isColumn(String columnName) {
        for (String column : columns.keySet()) {
            if (column.equals(column)) {
                return true;
            }
        }
        return false;
    }

    /**
     * returns the values in the table
     *
     * @param whereStatement
     * @return list of values
     */
    public List<Map<String, Object>> getTableValues(String whereStatement) {
        List<Map<String, Object>> returnValues = new ArrayList<>();
        if (whereStatement == null) {
            return this.values;
        }
        String[] condition = whereStatement.split("=");
        String conditionCol = condition[0].trim();
        String conditionVal = condition[1].trim();

        for (Map<String, Object> row : this.values) {
            if (row.containsKey(conditionCol) && String.valueOf(row.get(conditionCol)).equals((String) conditionVal)) {
                returnValues.add(row);
            }
        }

        return returnValues;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Table && ((Table) obj).getTableName().equals(this.tableName)) {
            return true;
        }
        return super.equals(obj);
    }

    /**
     * @return name of the table
     */
    public String getTableName() {
        return tableName;
    }
}