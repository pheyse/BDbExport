package de.bright_side.bdbexport.model;

import java.sql.Connection;
import java.util.Set;

import de.bright_side.bdbexport.bl.DbExporter.DbType;
import de.bright_side.bdbexport.bl.DbExporter.ObjectType;

public class BulkExportRequest {
	private Connection connection;
	private String userName;
	private String catalog;
	private String schema;
	private boolean sortTableData;
	private Set<ObjectType> objectTypes;
	private ObjectNameFilter tableDdlFilter;
	private ObjectNameFilter tableDataFilter;
	private ObjectNameFilter viewDdlFilter;
	private String whereClause;
	private DbType targetDbType;
	private int clobExportMaxLength;

	
	public Connection getConnection() {
		return connection;
	}
	public void setConnection(Connection connection) {
		this.connection = connection;
	}
	public String getUserName() {
		return userName;
	}
	
	/**
	 * if no value is set the user name is determined from the current connection
	 * @param userName user name
	 */
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public boolean isSortTableData() {
		return sortTableData;
	}
	public void setSortTableData(boolean sortTableData) {
		this.sortTableData = sortTableData;
	}
	public Set<ObjectType> getObjectTypes() {
		return objectTypes;
	}
	
	/**
	 * if no value is set, all object types are exported
	 * @param objectTypes set of object types to be exported
	 */
	public void setObjectTypes(Set<ObjectType> objectTypes) {
		this.objectTypes = objectTypes;
	}
	
	public String getWhereClause() {
		return whereClause;
	}
	public void setWhereClause(String whereClause) {
		this.whereClause = whereClause;
	}
	public DbType getTargetDbType() {
		return targetDbType;
	}
	
	/**
	 * if no value is set the target database type is assumed to be the same as the database type of the connection
	 * @param targetDbType target database type
	 */
	public void setTargetDbType(DbType targetDbType) {
		this.targetDbType = targetDbType;
	}
	public int getClobExportMaxLength() {
		return clobExportMaxLength;
	}
	
	/**
	 * if no value is set the default value DbExporter.DEFAULT_CLOB_EXPORT_MAX_LENGTH is used
	 * @param clobExportMaxLength maximum length of the CLOB export per line
	 */
	public void setClobExportMaxLength(int clobExportMaxLength) {
		this.clobExportMaxLength = clobExportMaxLength;
	}
	public String getCatalog() {
		return catalog;
	}

	/**
	 * if no value is set the catalog name is determined from the current connection. The catalog may also be irrelevant for some database types
	 * @param catalog catalog name
	 */
	public void setCatalog(String catalog) {
		this.catalog = catalog;
	}
	public String getSchema() {
		return schema;
	}
	/**
	 * if no value is set the schema name is determined from the current connection
	 * @param schema schema name
	 */
	public void setSchema(String schema) {
		this.schema = schema;
	}
	public ObjectNameFilter getTableDdlFilter() {
		return tableDdlFilter;
	}

	/**
	 * if no value is set objects with all names are exported. If the ObjectNameFilter.include or ObjectNameFilter.exclude is null, it means "all".
	 * @param tableDdlFilter table DDL filter: names of tables for which the DDL should be exported
	 */
	public void setTableDdlFilter(ObjectNameFilter tableDdlFilter) {
		this.tableDdlFilter = tableDdlFilter;
	}
	public ObjectNameFilter getTableDataFilter() {
		return tableDataFilter;
	}
	/**
	 * if no value is set objects with all names are exported. If the ObjectNameFilter.include or ObjectNameFilter.exclude is null, it means "all".
	 * @param tableDataFilter table data filter: names of tables of which the data should be be exported
	 */
	public void setTableDataFilter(ObjectNameFilter tableDataFilter) {
		this.tableDataFilter = tableDataFilter;
	}
	public ObjectNameFilter getViewDdlFilter() {
		return viewDdlFilter;
	}
	/**
	 * if no value is set objects with all names are exported. If the ObjectNameFilter.include or ObjectNameFilter.exclude is null, it means "all".
	 * @param viewDdlFilter view DDL filter: names of views to be exported
	 */
	public void setViewDdlFilter(ObjectNameFilter viewDdlFilter) {
		this.viewDdlFilter = viewDdlFilter;
	}

	
}
