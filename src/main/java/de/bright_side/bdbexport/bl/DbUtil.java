package de.bright_side.bdbexport.bl;


import static de.bright_side.bdbexport.bl.DbExportUtil.in;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import de.bright_side.bdbexport.bl.DbExporter.DbType;
import de.bright_side.bdbexport.model.CatalogAndSchema;
import de.bright_side.bdbexport.model.ReturnableValue;

public class DbUtil {
	private static final String H2_DATABASE_NAME = "H2";
	private static final String MS_SQL_SERVER_DATABASE_NAME = "Microsoft SQL Server";
	private static final String MY_SQL_DATABASE_NAME = "MySQL";
	private static final int POS_OF_DATABASE_NAME_AND_PARAMS_IN_URL = 3;
	
	public static List<String> getStringListQueryResult(Connection conn, String sql, String param1) throws Exception {
		PreparedStatement pstmt = null;
		List<String> returnList = new ArrayList<String>();
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, param1);
			ResultSet res = pstmt.executeQuery();
			while (res.next()) {
				returnList.add(res.getString(1));
			}
			res.close();
			return returnList;
		} catch (Exception e) {
			throw new Exception("Could not execute query >>" + sql + "<< (param1 = >>" + param1 + "<<)");
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
		}
	}

	public static List<String> getStringListQueryResult(Connection conn, String sql, String param1, String param2) throws Exception {
		PreparedStatement pstmt = null;
		List<String> returnList = new ArrayList<String>();
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, param1);
			pstmt.setString(2, param2);
			ResultSet res = pstmt.executeQuery();
			while (res.next()) {
				returnList.add(res.getString(1));
			}
			res.close();
			return returnList;
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
	}

	public static List<String> getStringListQueryResult(Connection conn, String sql, String param1, String param2, String param3) throws Exception {
		PreparedStatement pstmt = null;
		List<String> returnList = new ArrayList<String>();
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, param1);
			pstmt.setString(2, param2);
			pstmt.setString(3, param3);
			ResultSet res = pstmt.executeQuery();
			while (res.next()) {
				returnList.add(res.getString(1));
			}
			res.close();
			return returnList;
		} catch (Exception e) {
			throw new Exception("Could not execute query >>" + sql + "<< (param1 = >>" + param1 + "<<, param2 = >>" + param2 + "<<, param3 = >>" + param3 + "<<)");
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
		}
	}
	
	public static List<String> getStringListQueryResult(Connection conn, String sql) throws Exception {
		PreparedStatement pstmt = null;
		List<String> returnList = new ArrayList<String>();
		try {
			pstmt = conn.prepareStatement(sql);
			ResultSet res = pstmt.executeQuery();
			while (res.next()) {
				returnList.add(res.getString(1));
			}
			res.close();
			return returnList;
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
		}
	}

	public static List<String> getStringListQueryResultOfColumnWithName(Connection conn, String sql, String columnName) throws Exception {
		PreparedStatement pstmt = null;
		List<String> returnList = new ArrayList<String>();
		try {
			pstmt = conn.prepareStatement(sql);
			ResultSet res = pstmt.executeQuery();
			while (res.next()) {
				returnList.add(res.getString(columnName));
			}
			res.close();
			return returnList;
		} finally {
			if (pstmt != null)
				pstmt.close();
		}
	}

	public static String getStringQueryResult(Connection conn, String sql, String param1) throws Exception {
		String returnValue = null;
		PreparedStatement pstmt = null;
		ResultSet res = null;
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, param1);
			res = pstmt.executeQuery();
			if (res.next())
				returnValue = res.getString(1);
			return returnValue;
		} catch (Exception e) {
			throw new Exception("Could not execute query >>" + sql + "<< (param1 = >>" + param1 + "<<)");
		} finally {
			if (res != null) {
				res.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
		}
	}

	public static String getStringQueryResult(Connection conn, String sql, String param1, String param2) throws Exception {
		String returnValue = null;
		PreparedStatement pstmt = null;
		ResultSet res = null;
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, param1);
			pstmt.setString(2, param2);
			res = pstmt.executeQuery();
			if (res.next()) {
				returnValue = res.getString(1);
			}
			return returnValue;
		} catch (Exception e) {
			throw new Exception("Could not execute query >>" + sql + "<< (param1 = >>" + param1 + "<<, param2 = >>" + param2 + "<<)");
		} finally {
			if (res != null) {
				res.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
		}
	}

	public static String getStringQueryResult(Connection conn, String sql, String param1, String param2, String param3) throws Exception {
		String returnValue = null;
		PreparedStatement pstmt = null;
		ResultSet res = null;
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, param1);
			pstmt.setString(2, param2);
			pstmt.setString(3, param3);
			res = pstmt.executeQuery();
			if (res.next()) {
				returnValue = res.getString(1);
			}
			return returnValue;
		} catch (Exception e) {
			throw new Exception("Could not execute query >>" + sql + "<< (param1 = >>" + param1 + "<<, param2 = >>" + param2 + "<<, param3 = >>" + param3 + "<<)");
		} finally {
			if (res != null) {
				res.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
		}
	}
	
	public static String getStringQueryResult(Connection conn, String sql) throws Exception {
		String returnValue = null;
		PreparedStatement pstmt = null;
		ResultSet res = null;
		try {
			pstmt = conn.prepareStatement(sql);
			res = pstmt.executeQuery();
			if (res.next())
				returnValue = res.getString(1);
			return returnValue;
		} catch (Exception e) {
			throw new Exception("Could not execute query >>" + sql + "<<");
		} finally {
			if (res != null)
				res.close();
			if (pstmt != null)
				pstmt.close();
		}
	}

	public static String getStringQueryResultOfColumnWithName(Connection conn, String sql, String columnName) throws Exception {
		String returnValue = null;
		PreparedStatement pstmt = null;
		ResultSet res = null;
		try {
			pstmt = conn.prepareStatement(sql);
			res = pstmt.executeQuery();
			if (res.next()) {
				returnValue = res.getString(columnName);
			}
			return returnValue;
		} catch (Exception e) {
			throw new Exception("Could not execute query >>" + sql + "<<");
		} finally {
			if (res != null) {
				res.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
		}
	}

	public static List<String> getQueryResultAsSeparatedValues(Connection conn, String sql, String prefix, String suffix, String separator) throws Exception {
		List<String> result = new ArrayList<String>();
		PreparedStatement pstmt = null;
		ResultSet res = null;
		String line = "";
		try {
			pstmt = conn.prepareStatement(sql);
			res = pstmt.executeQuery();
			ResultSetMetaData metaData = res.getMetaData();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				if (i > 1) {
					line += separator;
				}
				line += prefix;
				line += metaData.getColumnName(i);
				line += suffix;
			}

			while (res.next()) {
				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					if (i > 1) {
						line += separator;
					}
					String value = res.getString(i);
					if (res.wasNull()) {
						value = null;
					}
					if (value != null) {
						line += prefix;
					}
					line += value;
					if (value != null) {
						line += suffix;
					}
				}
				result.add(line);
				line = "";
			}
			return result;
		} catch (Exception e) {
			throw new Exception("Could not execute query >>" + sql + "<<", e);
		} finally {
			if (res != null)
				res.close();
			if (pstmt != null)
				pstmt.close();
		}
	}

	public static SortedSet<String> getTypesToBeReadAsDate() {
		SortedSet<String> result = new TreeSet<String>();
		result.add("datetime");
		result.add("timestamp");
		result.add("date");
		result.add("datetime2");
		return result;
	}

	public static SortedSet<String> getTypesToBeReadAsString() {
		SortedSet<String> result = new TreeSet<String>();
		result.add("nvarchar");
		result.add("varchar");
		result.add("varchar2");
		result.add("char");
		return result;
	}

	public static SortedSet<String> getTypesToBeReadAsLong() {
		SortedSet<String> result = new TreeSet<String>();
		result.add("int");
		result.add("smallint");
		result.add("bigint");
		result.add("tinyint");
		result.add("number");
		return result;
	}

	public static SortedSet<String> getTypesToBeReadAsDouble() {
		SortedSet<String> result = new TreeSet<String>();
		result.add("float");
		result.add("double");
		result.add("decimal");
		return result;
	}

	public static SortedSet<String> getTypesToBeReadAsClob() {
		SortedSet<String> result = new TreeSet<String>();
		result.add("clob");
		return result;
	}

	public static String getDateOrNullAsSQLString(ResultSet resultSet, int columnIndex, SimpleDateFormat format, DbType dataBaseType) throws Exception {
		Timestamp value = resultSet.getTimestamp(columnIndex);
		if (resultSet.wasNull()) {
			return "NULL";
		} else {
			return "'" + format.format(value) + "'";
		}
	}

	public static String getNullOrStringAsSQLString(ResultSet resultSet, int columnIndex, DbType type) throws Exception {
		String value = resultSet.getString(columnIndex);
		if (resultSet.wasNull()) {
			return "NULL";
		}
		return getDBString(value, type);
	}
	
	public static String getNullOrStringAsSQLString(ResultSet resultSet, String columnName, DbType type) throws Exception{
		String value = resultSet.getString(columnName);
		if (resultSet.wasNull()) {
			return "NULL";
		}
		return getDBString(value, type);
	}
	
	public static String getDBString(String value, DbType type) throws Exception {
		if (value == null) {
			return "NULL";
		}
		if ((in(type, DbType.MS_SQL_SERVER, DbType.MY_SQL, DbType.MARIA_DB, DbType.H2))) {
			return "'" + value.replace("'", "''").replace("\r\n", "'+Char(13)+Char(10)+'").replace("\n", "'+Char(10)+'").replace("\r", "'+Char(13)+'").replace("'", "'+Char(39)+'")
					+ "'";
		} else
			throw new Exception("Unknown type: " + type);
	}

	public static String getLongOrNullAsSQLString(ResultSet resultSet, int columnIndex) throws Exception {
		long value = resultSet.getLong(columnIndex);
		if (resultSet.wasNull()) {
			return "NULL";
		}
		return "" + value;
	}

	public static String getDoubleOrNulllAsSQLString(ResultSet resultSet, int columnIndex) throws Exception {
		double value = resultSet.getDouble(columnIndex);
		if (resultSet.wasNull()) {
			return "NULL";
		}
		return "" + value;
	}

	public static String getClobOrNulllAsSQLString(ResultSet resultSet, int columnIndex, DbType type, int maximumSize) throws Exception {
		Clob clob = resultSet.getClob(columnIndex);
		if (resultSet.wasNull()) {
			return "NULL";
		}
		String string = clob.getSubString(1L, Math.min(maximumSize, (int) clob.length()));
		return getDBString(string, type);
	}

	public static DbType determineDbType(Connection connection) throws Exception {
		DatabaseMetaData metaData = connection.getMetaData();
		String databaseName = metaData.getDatabaseProductName();
		
		switch (databaseName) {
		case H2_DATABASE_NAME:
			return DbType.H2;
		case MS_SQL_SERVER_DATABASE_NAME:
			return DbType.MS_SQL_SERVER;
		case MY_SQL_DATABASE_NAME:
			return DbType.MY_SQL;
		default:
			throw new Exception("Could not determine the database type from the database product name '" + databaseName + "'");
		}
	}

	public static String determineUserName(Connection connection) throws Exception {
		return connection.getMetaData().getUserName();
	}

	public static SortedSet<String> getPrimaryKey(DatabaseMetaData dbMetaData, CatalogAndSchema catalogAndSchema, String tableName, ReturnableValue<String> pkName) throws SQLException {
		SortedSet<String> returnSet = new TreeSet<String>();
		String catalog = catalogAndSchema.getCatalog();
		String schema = catalogAndSchema.getSchema();
		ResultSet resultSet = null;
		try {
			resultSet = dbMetaData.getPrimaryKeys(catalog, schema, tableName);
			boolean first = true;
			while (resultSet.next()) {
				if (first) {
					pkName.setValue(resultSet.getString("PK_NAME"));
					first = false;
				}
				returnSet.add(resultSet.getString("COLUMN_NAME"));
			}
		} finally {
			if (resultSet != null) {
				resultSet.close();
			}
		}
		return returnSet;
	}

	public static List<String> toPartsSkipFirstPart(String string, int maximumLength) {
		List<String> returnList = new ArrayList<String>();
		String restString = string;
		boolean firstPart = true;
		while (restString.length() > 0){
			int lengthOfPart = Math.min(restString.length(), maximumLength);
			if (!firstPart)	returnList.add(restString.substring(0, lengthOfPart));
			firstPart = false;
			restString = restString.substring(lengthOfPart);
		}
		return returnList;
	}

	public static SortedSet<String> getDbMetaDataObjectNamesSortedAlphabetically(Connection connection, CatalogAndSchema catalogAndSchema, String objectTypeName) throws SQLException {
		SortedSet<String> result = new TreeSet<String>();
		DatabaseMetaData dmd = connection.getMetaData();
		ResultSet rs2 = dmd.getTables(catalogAndSchema.getCatalog(), catalogAndSchema.getSchema(), "%", null);
		while (rs2.next()) {
			if (rs2.getString(4).equalsIgnoreCase(objectTypeName)) {
				result.add(rs2.getString(3));
			}
		}
		return result;
	}

	public static String determineDatabaseNameFromJdbcUrl(Connection connection) throws Exception {
		DatabaseMetaData metaData = connection.getMetaData();
		if (metaData == null) {
			throw new Exception("Could not get database meta data");
		}
		
		String url = metaData.getURL();
		if (url == null) {
			throw new Exception("Could not get database JDBC URL");
		}
		
		String[] urlItems = url.split("/");
		
		if (urlItems.length != POS_OF_DATABASE_NAME_AND_PARAMS_IN_URL + 1) {
			throw new Exception("Could not read database name from url '" + url + "'");
		}
		
		String schemaNameAndParameters = urlItems[POS_OF_DATABASE_NAME_AND_PARAMS_IN_URL];
		int index = schemaNameAndParameters.indexOf("?");
		String databaseName = schemaNameAndParameters;
		if (index >= 0) {
			databaseName = schemaNameAndParameters.substring(0, index);
		}
		
		return databaseName;
	}
	
	public static CatalogAndSchema replaceEmptyWithDefaults(Connection connection, CatalogAndSchema catalogAndSchema) throws Exception {
		String catalog = null;
		if (catalogAndSchema != null) {
			catalog = catalogAndSchema.getCatalog();
		}
		if (catalog == null) {
			catalog = connection.getCatalog();
		}
		
		String schema = null;
		if (catalogAndSchema != null) {
			schema = catalogAndSchema.getSchema();
		}
		if (schema == null) {
			schema = connection.getSchema();
		}
		
		//: for MySQL and Maria DB the method connection.getSchema() returns the catalog and connection.getCatalog() returns the schema so they need to be switched!!
		DbType dbType = DbUtil.determineDbType(connection);
		if (in(dbType, DbType.MY_SQL, DbType.MARIA_DB)) {
			if ((catalogAndSchema != null) && (catalogAndSchema.getCatalog() == null) && (catalogAndSchema.getSchema() == null)) {
				String mem = schema;
				schema = catalog;
				catalog = mem;
			}
		}
		
		return new CatalogAndSchema(catalog, schema);
	}
	
}
