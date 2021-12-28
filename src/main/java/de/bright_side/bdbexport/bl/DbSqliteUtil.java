package de.bright_side.bdbexport.bl;

import java.sql.Connection;
import java.util.List;

public class DbSqliteUtil {
    public static final String SM_TYPE_TABLE = "table";
    public static final String SM_TYPE_VIEW = "view";
    public static final String SM_TYPE_INDEX = "index";
    private static final String SQLITE_MASTER = "sqlite_master";
    private static final String SM_SQL_COLUMN = "sql";
    private static final String SM_TABLE_TYPE_COLUMN = "type";
    private static final String SM_TABLENAME_COLUMN = "tbl_name";

    public static String readSqliteDdlForObject(Connection connection, String objectType, String objectName) throws Exception {
        String sql = "SELECT " + SM_SQL_COLUMN + " FROM " + SQLITE_MASTER + " WHERE " + SM_TABLE_TYPE_COLUMN + " = ? AND " + SM_TABLENAME_COLUMN + " = ?";
        return ensureTrainingSemicolon(DbUtil.getStringQueryResult(connection, sql, objectType, objectName));
    }

    public static String readSqliteIndexDdlsForTable(Connection connection, String objectName) throws Exception {
        StringBuilder result = new StringBuilder();
        String sql = "SELECT " + SM_SQL_COLUMN + " FROM " + SQLITE_MASTER + " WHERE " + SM_TABLE_TYPE_COLUMN + " = ? AND " + SM_TABLENAME_COLUMN + " = ?";
        List<String> indexSQLs = DbUtil.getStringListQueryResult(connection, sql, SM_TYPE_INDEX, objectName);

        for (String i: indexSQLs){
            if ((i != null) && (!i.equals("null"))){
                result.append("\n");
                result.append(ensureTrainingSemicolon(i));
                result.append("\n");
            }
        }

        return result.toString();
    }

    private static String ensureTrainingSemicolon(String text) {
        if (text.endsWith(";")){
            return text;
        }
        return text + ";";
    }
}
