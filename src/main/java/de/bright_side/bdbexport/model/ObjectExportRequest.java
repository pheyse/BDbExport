package de.bright_side.bdbexport.model;

import java.sql.Connection;

import de.bright_side.bdbexport.bl.DbExporter.DbType;
import de.bright_side.bdbexport.bl.DbExporter.ObjectType;

public class ObjectExportRequest {
	private Connection connection;
	private String userName;
	private boolean sortTableData;
	private ObjectType objectType;
	private String objectName; 
	private String whereClause;
	private DbType targetDbType;
	private int clobExportMaxLength;
	private String catalog;
	private String schema;
	
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
	public ObjectType getObjectType() {
		return objectType;
	}
	public void setObjectType(ObjectType objectType) {
		this.objectType = objectType;
	}
	public String getObjectName() {
		return objectName;
	}
	public void setObjectName(String objectName) {
		this.objectName = objectName;
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
	 * @param clobExportMaxLength maximum length per CLOB export line
	 */
	public void setClobExportMaxLength(int clobExportMaxLength) {
		this.clobExportMaxLength = clobExportMaxLength;
	}
	
	public String getCatalog() {
		return catalog;
	}
	
	/**
	 * if no value is set the catalog name is determined from the current connection. The catalog may also be irrelevant for some database types
	 * @param catalog name of the catalog
	 */
	public void setCatalog(String catalog) {
		this.catalog = catalog;
	}
	public String getSchema() {
		return schema;
	}
	/**
	 * if no value is set the schema name is determined from the current connection
	 * @param schema name of the schema
	 */
	public void setSchema(String schema) {
		this.schema = schema;
	}

}
