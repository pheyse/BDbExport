package de.bright_side.bdbexport.bl;

import static de.bright_side.bdbexport.bl.DbExportUtil.in;

import java.io.OutputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import de.bright_side.bdbexport.bl.DbExporter.DbType;
import de.bright_side.bdbexport.model.CatalogAndSchema;
import de.bright_side.bdbexport.model.InternalObjectExportRequest;

public class DbExportViews {
	
	/**
	 * @param connection JDBC connection
	 * @param catalog catalog name
	 * @param schema schema name 
	 * @return the view names in ordered by (approximated) view dependency
	 * @throws Exception thrown on errors
	 */
	public List<String> getViewNames(Connection connection, String catalog, String schema) throws Exception {
		return getViewNames(connection, new CatalogAndSchema(catalog, schema));
	}
	
	/**
	 * @param connection JDBC connection
	 * @param catalogAndSchema may not be null. Contains the catalog and the schema.
	 * @return the view names in ordered by (approximated) view dependency
	 * @throws Exception thrown on errors
	 */
	public List<String> getViewNames(Connection connection, CatalogAndSchema catalogAndSchema) throws Exception {
		DbType dbType = DbUtil.determineDbType(connection);
		SortedSet<String> viewNames = getViewNamesSortedAlphabetically(connection, catalogAndSchema);
		return sortViewsByDependencies(connection, dbType, catalogAndSchema, viewNames);
	}
	
	private SortedSet<String> getViewNamesSortedAlphabetically(Connection connection, CatalogAndSchema catalogAndSchema) throws Exception {
		DbType dbType = DbUtil.determineDbType(connection);
		if (in(dbType, DbType.H2, DbType.MY_SQL, DbType.MARIA_DB)) {
			return DbUtil.getDbMetaDataObjectNamesSortedAlphabetically(connection, catalogAndSchema, "VIEW");
		} else if (dbType == DbType.MS_SQL_SERVER) {
			return getMsSqlServerViewNamesSortedAlphabetically(connection, catalogAndSchema);
		} else {
			throw new Exception("Unknown data base type: '" + dbType + "'");
		}
	}

	private SortedSet<String> getMsSqlServerViewNamesSortedAlphabetically(Connection connection, CatalogAndSchema catalogAndSchema) throws Exception {
		String sql = "select TABLE_NAME from [" + catalogAndSchema.getCatalog() + "].INFORMATION_SCHEMA.VIEWS where TABLE_CATALOG=? and TABLE_SCHEMA=? order by TABLE_NAME";
		List<String> resultList = DbUtil.getStringListQueryResult(connection, sql, catalogAndSchema.getCatalog(), catalogAndSchema.getSchema());
		return new TreeSet<String>(resultList);
	}

	private List<String> sortViewsByDependencies(Connection connection, DbType dataBaseType, CatalogAndSchema catalogAndSchema, Collection<String> viewNames) throws Exception {
		List<String> returnList = new ArrayList<String>();
		List<String> useViewNames = new ArrayList<String>();
		for (String i : viewNames) {
			useViewNames.add(i.toUpperCase());
		}
		SortedSet<String> doneViews = new TreeSet<String>();
		SortedSet<String> toDoViews = new TreeSet<String>(useViewNames);

		SortedMap<String, SortedSet<String>> dependencies = new TreeMap<String, SortedSet<String>>();
		for (String i : toDoViews) {
			SortedSet<String> dependentViews = getViewsThatThisViewDependsOn(connection, dataBaseType, catalogAndSchema, i, useViewNames);
			dependencies.put(i, dependentViews);
		}

		int lastToDoViewSize = 0;
		while (toDoViews.size() > 0) {
			lastToDoViewSize = toDoViews.size();
			for (String i : toDoViews) {
				if (doneViews.containsAll(dependencies.get(i))) {
					returnList.add(i);
					doneViews.add(i);
				} else {
				}
			}
			toDoViews.removeAll(doneViews);
			if (toDoViews.size() == lastToDoViewSize) {
				//: do not throw an error, just add the first view...
				returnList.add(toDoViews.first());
				doneViews.add(toDoViews.first());
			}
			toDoViews.removeAll(doneViews);
		}
		return returnList;
	}

	private SortedSet<String> getViewsThatThisViewDependsOn(Connection connection, DbType dataBaseType, CatalogAndSchema catalogAndSchema, String viewName,
			Collection<String> potentialViewNames) throws Exception {
		SortedSet<String> returnSet = new TreeSet<String>();
		String sql = getViewDdl(connection, dataBaseType, catalogAndSchema, viewName).toUpperCase();
		for (String i : potentialViewNames) {
			if (sql.contains(i.toUpperCase()))
				returnSet.add(i.toUpperCase());
		}
		return returnSet;
	}

	
	public String getViewDdl(Connection connection, DbType dataBaseType, CatalogAndSchema catalogAndSchema, String viewName) throws Exception{
		if (dataBaseType == DbType.H2){
			return createH2ViewDdl(connection, catalogAndSchema, viewName);
		} else if (dataBaseType == DbType.MS_SQL_SERVER){
			return createMsSqlServerViewDdl(connection, catalogAndSchema, viewName);
		} else if (in (dataBaseType, DbType.MY_SQL, DbType.MARIA_DB)){
			return createMySqlViewDdl(connection, catalogAndSchema, viewName);
		} else{
			throw new Exception("Not implemented for db type " + dataBaseType);
		}
	}

	private String createMySqlViewDdl(Connection connection, CatalogAndSchema catalogAndSchema, String viewName) throws Exception {
		String result = DbUtil.getStringQueryResultOfColumnWithName(connection, "show create view " + catalogAndSchema.getSchema() + "." + viewName, "Create View");
		return result;
	}

	private String createMsSqlServerViewDdl(Connection connection, CatalogAndSchema catalogAndSchema, String viewName) throws Exception {
		List<String> listResult = DbUtil.getStringListQueryResult(connection, "sp_helptext '[" + catalogAndSchema.getCatalog() + "]." + catalogAndSchema.getSchema() + "." + viewName + "'");
		return DbExportUtil.collectionToString(listResult, "");
	}

	public void exportViewDdl(InternalObjectExportRequest exportRequest, OutputStream outputStream) throws Exception {
		String result = getViewDdl(exportRequest.getConnection(), exportRequest.getDbType(), exportRequest.getCatalogAndSchema(), exportRequest.getObjectName());
		DbExportUtil.write(outputStream, result);
	}
	
	private String createH2ViewDdl(Connection connection, CatalogAndSchema catalogAndSchema, String viewName) throws Exception {
		String result;
		String sql = "SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_CATALOG = ? and TABLE_SCHEMA = ? and TABLE_NAME = ?";
		result = DbUtil.getStringQueryResult(connection, sql, catalogAndSchema.getCatalog(), catalogAndSchema.getSchema(), viewName);
		result += ";\n";
		return result;
	}
	
}
