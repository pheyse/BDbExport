package de.bright_side.bdbexport.model;

public class CatalogAndSchema {
	private String catalog;
	private String schema;
	
	public CatalogAndSchema(String catalog, String schema) {
		super();
		this.catalog = catalog;
		this.schema = schema;
	}
	
	public String getCatalog() {
		return catalog;
	}
	public void setCatalog(String catalog) {
		this.catalog = catalog;
	}
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	@Override
	public String toString() {
		return "CatalogAndSchema [catalog=" + catalog + ", schema=" + schema + "]";
	}
	
}
