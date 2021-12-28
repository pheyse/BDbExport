package de.bright_side.bdbexport.bl;

import static de.bright_side.bdbexport.bl.DbExportUtil.in;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import de.bright_side.bdbexport.model.BulkExportRequest;
import de.bright_side.bdbexport.model.CancelReceiver;
import de.bright_side.bdbexport.model.CatalogAndSchema;
import de.bright_side.bdbexport.model.InternalObjectExportRequest;
import de.bright_side.bdbexport.model.ObjectExportRequest;

public class DbExporter {
	public static final Charset CHARSET = Charset.forName("UTF-8");
	public static final int DEFAULT_CLOB_EXPORT_MAX_LENGTH = 1024;
	
	public enum DbType {MS_SQL_SERVER, H2, MY_SQL, MARIA_DB, SQLITE}
	public enum ObjectType{TABLE_DDL, TABLE_DATA, VIEW_DDL}
	
	/**
	 * @param connection JDBC connection
	 * @param objectType type of object to be exported
	 * @param catalog null for default
	 * @param schema null for default 
	 * @return object names ordered by dependencies (approximated)
	 * @throws Exception thrown on errors
	 */
	public List<String> getObjectNames(Connection connection, ObjectType objectType, String catalog, String schema) throws Exception{
		return getObjectNames(connection, objectType, new CatalogAndSchema(catalog, schema));
	}
	
	/**
	 * 
	 * @param connection JDBC connection
	 * @param objectType type of object to be exported
	 * @param catalogAndSchema may not be null but "inside" catalog and/or schema may be null in order to use the default values
	 * @return object names ordered by dependencies (approximated)
	 * @throws Exception thrown on errors
	 */
	private List<String> getObjectNames(Connection connection, ObjectType objectType, CatalogAndSchema catalogAndSchema) throws Exception{
		CatalogAndSchema useCatalogAndSchema = DbUtil.replaceEmptyWithDefaults(connection, catalogAndSchema);
		if (in(objectType, ObjectType.TABLE_DATA, ObjectType.TABLE_DDL)) {
			return new DbExportTables().getTableNames(connection, useCatalogAndSchema);
		} else if (objectType == ObjectType.VIEW_DDL) {
			return new DbExportViews().getViewNames(connection, useCatalogAndSchema);
		}
		throw new Exception("Unknown object type: " + objectType);
	}

	public void objectExport(ObjectExportRequest exportRequest, OutputStream outputStream) throws Exception {
		objectExport(exportRequest, outputStream, new CancelReceiver());
	}
	
	/**
	 * 
	 * @param exportRequest connection and settings for the export
	 * @param outputStream the exported data is written to the output stream
	 * @param cancelReceiver the method cancel() may be called while the export is running (e.g. in a different thread while exporting a large table)
	 * @throws Exception thrown on errors
	 */
	public void objectExport(ObjectExportRequest exportRequest, OutputStream outputStream, CancelReceiver cancelReceiver) throws Exception {
		InternalObjectExportRequest internalExportRequest = DbExportUtil.toInternalRequest(exportRequest);
		
		switch (exportRequest.getObjectType()) {
		case TABLE_DDL:
			new DbExportTables().exportTableDdl(internalExportRequest, outputStream);
			break;
		case TABLE_DATA:
			new DbExportTables().exportTableData(internalExportRequest, cancelReceiver, outputStream);
			break;
		case VIEW_DDL:
			new DbExportViews().exportViewDdl(internalExportRequest, outputStream);
			break;
		default:
			throw new Exception("Unknown object type: " + exportRequest.getObjectType());
		}
	}
	
	/**
	 * 
	 * @param exportRequest connection and settings for the export
	 * @return the object data as a string
	 * @throws Exception thrown on errors
	 */
	public String objectExportAsString(ObjectExportRequest exportRequest) throws Exception {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		objectExport(exportRequest, outputStream);
		return new String(outputStream.toByteArray(), CHARSET);
	}
	
	/**
	 * 
	 * @param exportRequest connection and settings for the export
	 * @param cancelReceiver the method cancel() may be called while the export is running (e.g. in a different thread while exporting many objects). May also be null.
	 * @param outputStream the exported data is written to the output stream
	 * @throws Exception thrown on errors
	 */
	public void bulkExport(BulkExportRequest exportRequest, CancelReceiver cancelReceiver, OutputStream outputStream) throws Exception {
    	List<String> objectNames;
    	
    	CancelReceiver useCancelReceiver = cancelReceiver;
    	if (useCancelReceiver == null) {
    		useCancelReceiver = new CancelReceiver();
    	}
    	
    	List<ObjectType> useObjectTypes = new ArrayList<ObjectType>();
    	
    	//: iterate over the object type in the order of the enum which is an order that works for export
    	for (ObjectType i: ObjectType.values()) {
    		if ((exportRequest.getObjectTypes() == null) || (exportRequest.getObjectTypes().contains(i))) {
    			useObjectTypes.add(i);
    		}
    	}
    	
    	for (ObjectType objectType: useObjectTypes) {
    		try {
    			DbExportUtil.write(outputStream, "\n\n-- ####################################################\n");
    			DbExportUtil.write(outputStream, "-- Exporting type " + ("" + objectType).replace("_", " ") + ":\n");
    			DbExportUtil.write(outputStream, "-- ####################################################\n\n");
    			objectNames = getObjectNames(exportRequest.getConnection(), objectType, exportRequest.getCatalog(), exportRequest.getSchema());
    			for (String objectName: objectNames) {
    				if (useCancelReceiver.wantToCancel()) {
    					return;
    				}
    				if (DbExportUtil.matches(exportRequest, objectType, objectName)) {
    					ObjectExportRequest objectExportRequest = new ObjectExportRequest();
    					objectExportRequest.setConnection(exportRequest.getConnection());
    					objectExportRequest.setObjectName(objectName);
    					objectExportRequest.setObjectType(objectType);
    					objectExportRequest.setSortTableData(true);
    					try {
    						objectExport(objectExportRequest, outputStream, useCancelReceiver);
    					} catch (Exception e) {
    						throw new Exception("Error while exporting object '" + objectName + "' of type " + objectType, e);
    					}
    				}
    			}
    			DbExportUtil.write(outputStream, "\n\n");
    		} catch (Exception e) {
    			throw new Exception("Error while exporting objects of type " + objectType, e);
    		}
    	}
	}

	/**
	 * 
	 * @param exportRequest connection and settings for the export
	 * @param cancelReceiver the method cancel() may be called while the export is running (e.g. in a different thread while exporting many objects)
	 * @return string that contains the exported data
	 * @throws Exception thrown on errors
	 */
	public String bulkExportAsString(BulkExportRequest exportRequest, CancelReceiver cancelReceiver) throws Exception {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		bulkExport(exportRequest, cancelReceiver, outputStream);
		return new String(outputStream.toByteArray(), CHARSET);
	}

	/**
	 * 
	 * @param exportRequest connection and settings for the export
	 * @return string that contains the exported data
	 * @throws Exception thrown on errors
	 */
	public String bulkExportAsString(BulkExportRequest exportRequest) throws Exception {
		return bulkExportAsString(exportRequest, new CancelReceiver());
	}

}
