package de.bright_side.bdbexport.bl;

import static de.bright_side.bdbexport.bl.DbExportUtil.in;

import java.io.OutputStream;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import de.bright_side.bdbexport.bl.DbExporter.DbType;
import de.bright_side.bdbexport.model.CancelReceiver;
import de.bright_side.bdbexport.model.CatalogAndSchema;
import de.bright_side.bdbexport.model.InternalObjectExportRequest;
import de.bright_side.bdbexport.model.ReturnableValue;

public class DbExportTables {
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	private static final SortedSet<String> TYPES_TO_READ_AS_DATE = DbUtil.getTypesToBeReadAsDate();
	private static final SortedSet<String> TYPES_TO_READ_AS_STRING = DbUtil.getTypesToBeReadAsString();
	private static final SortedSet<String> TYPES_TO_READ_AS_LONG = DbUtil.getTypesToBeReadAsLong();
	private static final SortedSet<String> TYPES_TO_READ_AS_DOUBLE = DbUtil.getTypesToBeReadAsDouble();
	private static final SortedSet<String> TYPES_TO_READ_AS_CLOB = DbUtil.getTypesToBeReadAsClob();
	private static final String MY_SQL_DEFAULT_CATALOG_NAME = "def";

	private SortedSet<String> getTableNamesSortedAlphabetically(Connection connection, CatalogAndSchema catalogAndSchema) throws Exception {
		DbType dbType = DbUtil.determineDbType(connection);
		if (in(dbType, DbType.H2)) {
			return DbUtil.getDbMetaDataObjectNamesSortedAlphabetically(connection, catalogAndSchema, "TABLE");
		} else if (dbType == DbType.MS_SQL_SERVER) {
			return getMsSqlServerTableNamesSortedAlphabetically(connection, catalogAndSchema);
		} else if (in(dbType, DbType.MY_SQL, DbType.MARIA_DB)) {
			return getMySqlMariaDbTableNamesSortedAlphabetically(connection, catalogAndSchema);
		} else {
			throw new Exception("Unknown data base type: '" + dbType + "'");
		}
	}

	private SortedSet<String> getMsSqlServerTableNamesSortedAlphabetically(Connection connection, CatalogAndSchema catalogAndSchema) throws Exception {
		String sql = "select TABLE_NAME from [" + catalogAndSchema.getCatalog() + "].INFORMATION_SCHEMA.TABLES where TABLE_CATALOG=? and TABLE_SCHEMA=? and TABLE_TYPE = 'BASE TABLE' order by TABLE_NAME";
		List<String> resultList = DbUtil.getStringListQueryResult(connection, sql, catalogAndSchema.getCatalog(), catalogAndSchema.getSchema());
		return new TreeSet<String>(resultList);
	}

	private SortedSet<String> getMySqlMariaDbTableNamesSortedAlphabetically(Connection connection, CatalogAndSchema catalogAndSchema) throws Exception {
		String useCatalog = catalogAndSchema.getCatalog();
		if (useCatalog == null) {
			useCatalog = MY_SQL_DEFAULT_CATALOG_NAME;
		}
		String sql = "select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_CATALOG = ? AND TABLE_SCHEMA = ? and TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME";
		List<String> resultList = DbUtil.getStringListQueryResult(connection, sql, useCatalog, catalogAndSchema.getSchema());
		return new TreeSet<String>(resultList);
	}
	
	private List<String> sortTablesByDependencies(Connection connection, DbType dbType, DatabaseMetaData dbMetaData, CatalogAndSchema catalogAndSchema, Collection<String> tableNames) throws Exception {
		List<String> returnList = new ArrayList<String>();
		SortedSet<String> doneTables = new TreeSet<String>();
		SortedSet<String> toDoTables = new TreeSet<String>(tableNames);

		SortedMap<String, SortedSet<String>> dependencies = new TreeMap<String, SortedSet<String>>();
		for (String i : toDoTables) {
			SortedSet<String> dependentTables = getTablesThatThisTableDependsOn(connection, dbType, dbMetaData, catalogAndSchema, i);
			dependentTables.retainAll(tableNames);
			dependencies.put(i, dependentTables);
		}

		int lastToDoTableSize = 0;
		while (toDoTables.size() > 0) {
			lastToDoTableSize = toDoTables.size();
			for (String i : toDoTables) {
				if (doneTables.containsAll(dependencies.get(i))) {
					returnList.add(i);
					doneTables.add(i);
				}
			}
			toDoTables.removeAll(doneTables);
			if (toDoTables.size() == lastToDoTableSize) {
				throw new Exception("Could not resolve all table dependencies to determine export order");
			}
		}

		return returnList;
	}

	private static SortedSet<String> getTablesThatThisTableDependsOn(Connection connection, DbType dbType, DatabaseMetaData dbMetaData, CatalogAndSchema catalogAndSchema, String tableName) throws Exception {
		if (in(dbType, DbType.H2)) {
			return getTablesThatThisTableDependsOnH2(connection, dbMetaData, catalogAndSchema, tableName);
		} else if (in(dbType, DbType.MS_SQL_SERVER)) {
			return getTablesThatThisTableDependsOnMsSqlServer(connection, catalogAndSchema, tableName);
		} else if (in(dbType, DbType.MY_SQL, DbType.MARIA_DB)) {
			return getTablesThatThisTableDependsOnMySql(connection, catalogAndSchema, tableName);
		} else {
			throw new Exception("Unknown database type: " + dbType);
		}
	}

	private static SortedSet<String> getTablesThatThisTableDependsOnMySql(Connection connection, CatalogAndSchema catalogAndSchema, String tableName) throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT rc.REFERENCED_TABLE_NAME FROM information_schema.table_constraints tc\n");
		sb.append(" left join information_schema.REFERENTIAL_CONSTRAINTS rc \n");
		sb.append("        on tc.CONSTRAINT_NAME  = rc.CONSTRAINT_NAME \n");
		sb.append("       and tc.CONSTRAINT_CATALOG  = rc.CONSTRAINT_CATALOG \n");
		sb.append("WHERE tc.table_schema = ?\n");
		sb.append("and tc.CONSTRAINT_CATALOG = ?\n");
		sb.append("AND tc.table_name = ?\n");
		sb.append("AND tc.constraint_type='FOREIGN KEY';\n");
		String sql = sb.toString();
		
		String schema = catalogAndSchema.getSchema();
		String catalog = catalogAndSchema.getCatalog();
		if (catalog == null) {
			catalog = MY_SQL_DEFAULT_CATALOG_NAME;
		}
		
		List<String> listResult = DbUtil.getStringListQueryResult(connection, sql, schema, catalog, tableName);
		return new TreeSet<String>(listResult);
	}

	private static SortedSet<String> getTablesThatThisTableDependsOnMsSqlServer(Connection connection, CatalogAndSchema catalogAndSchema, String tableName) throws Exception {
		String sql = DbExportTableScripts.getMsSqlServerTableDependenciesScript();
		sql = sql.replace(DbExportTableScripts.CATALOG_NAME_PLACEHOLDER, catalogAndSchema.getCatalog());
		String schema = "dbo";
		if (catalogAndSchema.getSchema() != null) {
			schema = catalogAndSchema.getSchema();
		}
		sql = sql.replace(DbExportTableScripts.SCHEMA_NAME_PLACEHOLDER, schema);
		sql = sql.replace(DbExportTableScripts.TABLE_NAME_PLACEHOLDER, tableName);
		
		List<String> listResult = DbUtil.getStringListQueryResult(connection, sql);
		
		return new TreeSet<String>(listResult);
	}
	
	private static SortedSet<String> getTablesThatThisTableDependsOnH2(Connection connection, DatabaseMetaData dbMetaData, CatalogAndSchema catalogAndSchema, String tableName) throws Exception {
		String sql = "select PKTABLE_NAME from INFORMATION_SCHEMA.CROSS_REFERENCES where FKTABLE_NAME = ?";
		List<String> listResult = DbUtil.getStringListQueryResult(connection, sql, tableName);
		return new TreeSet<String>(listResult);
	}
	
	@SuppressWarnings("unused")
	private static SortedSet<String> getTablesThatThisTableDependsOnViaJdbc(DatabaseMetaData dbMetaData, CatalogAndSchema catalogAndSchema, String tableName)
			throws Exception, SQLException {
		SortedSet<String> returnSet = new TreeSet<String>(); 
		String catalog = catalogAndSchema.getCatalog();
		String schema = catalogAndSchema.getSchema();
		
		ResultSet resultSet = null;
		try {
			resultSet = dbMetaData.getCrossReference(catalog, schema, "%", catalog, schema, tableName);
			while (resultSet.next()) {
				returnSet.add(resultSet.getString("PKTABLE_NAME"));
			}
		} catch (Exception e) {
			throw new Exception("Could not get cross reference data for catalog '" + catalog + "', schema '" + schema + "', tableName '" + tableName + "'", e);
		} finally {
			if (resultSet != null) {
				resultSet.close();
			}
		}
		
		return returnSet;
	}

	/**
	 * @param connection JDBC connection
	 * @param catalogAndSchema may not be null. Contains the catalog and the schema to be exported
	 * @return table names sorted by dependency
	 * @throws Exception thrown if an error occurs
	 */
	public List<String> getTableNames(Connection connection, CatalogAndSchema catalogAndSchema) throws Exception {
		SortedSet<String> tableNames = getTableNamesSortedAlphabetically(connection, catalogAndSchema);
		DatabaseMetaData dbMetaData = connection.getMetaData();
		DbType dbType = DbUtil.determineDbType(connection);
		return sortTablesByDependencies(connection, dbType, dbMetaData, catalogAndSchema, tableNames);
	}

	public void exportTableDdl(InternalObjectExportRequest exportRequest, OutputStream outputStream) throws Exception {
		String result;
		switch (exportRequest.getDbType()) {
		case H2:
			result = createH2TableDdl(exportRequest);
			break;
		case MS_SQL_SERVER:
			result = createMsSqlServerTableDdl(exportRequest);
			break;
		case MY_SQL:
			result = createMySqlOrMariaDbServerTableDdl(exportRequest);
			break;
		case MARIA_DB:
			result = createMySqlOrMariaDbServerTableDdl(exportRequest);
			break;
		default:
			throw new Exception("Database type not supported for export table DDL: " + exportRequest.getDbType());
		}
		result += "\n\n";
		DbExportUtil.write(outputStream, result);
	}

	private String createMsSqlServerTableDdl(InternalObjectExportRequest exportRequest) throws Exception {
		Connection connection = exportRequest.getConnection();
		String result;
		String sql = DbExportTableScripts.getMsSqlServerCreateTableDdlScript();
		sql = sql.replace(DbExportTableScripts.CATALOG_NAME_PLACEHOLDER, exportRequest.getCatalogAndSchema().getCatalog());
		sql = sql.replace(DbExportTableScripts.TABLE_NAME_PLACEHOLDER, exportRequest.getObjectName());
		result = DbUtil.getStringQueryResult(connection, sql);
		result += "\n";
		return result;
	}

	private String createMySqlOrMariaDbServerTableDdl(InternalObjectExportRequest exportRequest) throws Exception {
		Connection connection = exportRequest.getConnection();
		return DbUtil.getStringQueryResultOfColumnWithName(connection, "show create table " + exportRequest.getCatalogAndSchema().getSchema() + "." + exportRequest.getObjectName(), "Create Table");
	}

	private String createH2TableDdl(InternalObjectExportRequest exportRequest) throws Exception {
		String result;
		Connection connection = exportRequest.getConnection();
		CatalogAndSchema catalogAndSchema = exportRequest.getCatalogAndSchema();
		String sql = "SELECT SQL FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = ? and TABLE_SCHEMA = ? and TABLE_NAME = ?";
		result = DbUtil.getStringQueryResult(connection, sql, catalogAndSchema.getCatalog(), catalogAndSchema.getSchema(), exportRequest.getObjectName());
		result += ";\n";
		return result;
	}
	
	public void exportTableData(InternalObjectExportRequest exportRequest, CancelReceiver cancelReceiver, OutputStream outputStream) throws Exception {
		exportTableDataAsInsertStatements(exportRequest, cancelReceiver, outputStream);
	}

	private void exportTableDataAsInsertStatements(InternalObjectExportRequest exportRequest, CancelReceiver cancelReceiver, OutputStream output) throws Exception {
		ResultSet resultSet = null;
		Statement statement = null;
		try {
			Connection connection = exportRequest.getConnection();
			statement = connection.createStatement();
			String tableName = exportRequest.getObjectName();
			String whereClause = generateWhereClauseString(exportRequest.getWhereClause());
			resultSet = statement.executeQuery("SELECT * FROM " + tableName);
			DatabaseMetaData dbMetaData = connection.getMetaData();

			ResultSetMetaData metaData = resultSet.getMetaData();
			int numberOfColumns = metaData.getColumnCount();
			List<String> columnNames = new ArrayList<String>();
			List<String> columnNamesAndTypes = new ArrayList<String>();
			List<String> sortColumns = new ArrayList<String>();

			for (int i = 1; i <= numberOfColumns; i++) {
				String columnName = metaData.getColumnName(i);
				String type = metaData.getColumnTypeName(i).toLowerCase();
				columnNames.add(columnName);
				columnNamesAndTypes.add("'" + metaData.getColumnName(i) + "'(type=" + type + ")");
				if (!TYPES_TO_READ_AS_CLOB.contains(type)) {
					sortColumns.add(columnName); // cannot sort by COLB columns
				}
			}
			ReturnableValue<String> pkName = new ReturnableValue<String>("");
			SortedSet<String> primaryKeyItems = DbUtil.getPrimaryKey(dbMetaData, exportRequest.getCatalogAndSchema(), tableName, pkName);
			DbExportUtil.write(output, "-- table '" + tableName + "'\n");
			DbExportUtil.write(output, "-- columns=" + DbExportUtil.collectionToString(columnNamesAndTypes, ", ") + "\n");
			DbExportUtil.write(output, "-- primary key: name = '" + pkName.getValue() + "' columns = {" + DbExportUtil.collectionToString(primaryKeyItems, ", ") + "}\n");

			String useTableColumnQuote = "\"";
			if (in(exportRequest.getTargetDbType(), DbType.MY_SQL, DbType.MARIA_DB)) {
				useTableColumnQuote = "";
			}

			if (exportRequest.isSortTableData()) {
				resultSet.close();
				String sql = "SELECT * FROM " + tableName + generateWhereClauseString(whereClause) + " " + generateOrderByString(sortColumns);
				DbExportUtil.write(output, "-- SQL: " + sql.replace("\r", "").replace("\n", " ") + "\n");
				try {
					resultSet = statement.executeQuery(sql);
				} catch (Exception e) {
					throw new Exception("Could not run query with sorted rows: >>" + sql + "<<", e);
				}
				metaData = resultSet.getMetaData();
			}

			long numberOfRows = 0;
			while ((resultSet.next() && (!cancelReceiver.wantToCancel()))) {
				DbExportUtil.write(output, "INSERT INTO " + tableName + "(" + DbExportUtil.collectionToString(columnNames, useTableColumnQuote, useTableColumnQuote, ", ") + ") VALUES (");

				SortedSet<Integer> clobColumnsToWrite = new TreeSet<Integer>();
				DbType sourceDataBaseType = exportRequest.getDbType();
				for (int i = 1; i <= numberOfColumns; i++) {
					if (i != 1) {
						DbExportUtil.write(output, ", ");
					}
					String type = metaData.getColumnTypeName(i).toLowerCase();
					if (type == null) {
						throw new Exception("Unknown type for column # " + i + ": null");
					}

					if (TYPES_TO_READ_AS_DATE.contains(type)) {
						DbExportUtil.write(output, DbUtil.getDateOrNullAsSQLString(resultSet, i, DATE_FORMAT, sourceDataBaseType));
					} else if (TYPES_TO_READ_AS_STRING.contains(type)) {
						DbExportUtil.write(output, DbUtil.getNullOrStringAsSQLString(resultSet, i, sourceDataBaseType));
					} else if (TYPES_TO_READ_AS_LONG.contains(type)) {
						DbExportUtil.write(output, DbUtil.getLongOrNullAsSQLString(resultSet, i));
					} else if (TYPES_TO_READ_AS_DOUBLE.contains(type)) {
						DbExportUtil.write(output, DbUtil.getDoubleOrNulllAsSQLString(resultSet, i));
					} else if (TYPES_TO_READ_AS_CLOB.contains(type)) {
						clobColumnsToWrite.add(i);
						DbExportUtil.write(output, DbUtil.getClobOrNulllAsSQLString(resultSet, i, sourceDataBaseType, exportRequest.getClobExportMaxLength()));
					} else {
						throw new Exception("Don't know how to write data of type '" + type + "' into insert statement");
					}
				}
				DbExportUtil.write(output, ");\n");

				if (clobColumnsToWrite.size() > 0) {
					if ((primaryKeyItems.size() == 0)) {
						throw new Exception("CLOB larger than values " + exportRequest.getClobExportMaxLength() + " characters cannot be exported if there is no primary key. "
								+ "(Because multiple update statements are needed to insert a CLOB value but the rows cannot be determined" + " without a primary key)");
					}
					List<String> whereStringItems = new ArrayList<String>();
					for (String i : primaryKeyItems) {
						whereStringItems.add(" " + i + " = " + DbUtil.getNullOrStringAsSQLString(resultSet, i, sourceDataBaseType) + " ");
					}
					String whereString = " where " + DbExportUtil.collectionToString(whereStringItems, " AND ");
					for (Integer clobColumnIndex : clobColumnsToWrite) {
						String columnName = metaData.getColumnName(clobColumnIndex);
						String clobString = "";
						Clob clob = resultSet.getClob(clobColumnIndex);
						clobString = clob.getSubString(1L, (int) clob.length());
						List<String> clobDataParts = DbUtil.toPartsSkipFirstPart(clobString, exportRequest.getClobExportMaxLength());
						for (String clobDataPart : clobDataParts) {
							String clobLine = "UPDATE " + tableName + " SET " + columnName + " = " + columnName + " || "
									+ DbUtil.getDBString(clobDataPart, exportRequest.getTargetDbType()) + whereString + ";\n";
							DbExportUtil.write(output, clobLine);
						}
					}
				}

				numberOfRows++;
			}
			DbExportUtil.write(output, "-- " + numberOfRows + " row(s) exported\n\n");

		} catch (Exception e) {
			throw e;
		} finally {
			if (resultSet != null) {
				resultSet.close();
			}
			if (statement != null) {
				statement.close();
			}
		}
	}

	private String generateWhereClauseString(String whereClause) {
		if ((whereClause == null) || (whereClause.isEmpty())) {
			return "";
		}
		return " WHERE " + whereClause;
	}

	private String generateOrderByString(List<String> columns) {
		if (columns.size() == 0)
			return " ";
		return "ORDER BY " + DbExportUtil.collectionToString(columns, "\"", "\"", ", ");
	}

}
