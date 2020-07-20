package de.bright_side.bdbexport.model;

import java.sql.Connection;

import de.bright_side.bdbexport.bl.DbExporter.DbType;
import de.bright_side.bdbexport.bl.DbExporter.ObjectType;

public class InternalObjectExportRequest {
	private Connection connection;
	private DbType dbType;
	private String userName;
	private CatalogAndSchema catalogAndSchema;
	private boolean sortTableData;
	private ObjectType objectType;
	private String objectName; 
	private String whereClause;
	private DbType targetDbType;
	private int clobExportMaxLength;

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

	public void setTargetDbType(DbType targetDbType) {
		this.targetDbType = targetDbType;
	}

	public int getClobExportMaxLength() {
		return clobExportMaxLength;
	}

	public void setClobExportMaxLength(int clobExportMaxLength) {
		this.clobExportMaxLength = clobExportMaxLength;
	}

	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	public DbType getDbType() {
		return dbType;
	}

	public void setDbType(DbType dbType) {
		this.dbType = dbType;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public CatalogAndSchema getCatalogAndSchema() {
		return catalogAndSchema;
	}

	public void setCatalogAndSchema(CatalogAndSchema catalogAndSchema) {
		this.catalogAndSchema = catalogAndSchema;
	}

	@Override
	public String toString() {
		return "InternalObjectExportRequest [dbType=" + dbType + ", userName=" + userName + ", catalogAndSchema=" + catalogAndSchema + ", sortTableData=" + sortTableData
				+ ", objectType=" + objectType + ", objectName=" + objectName + ", whereClause=" + whereClause + ", targetDbType=" + targetDbType + ", clobExportMaxLength="
				+ clobExportMaxLength + "]";
	}

}
