package de.bright_side.bdbexport.bl;

import java.io.OutputStream;
import java.sql.Connection;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import de.bright_side.bdbexport.bl.DbExporter.ObjectType;
import de.bright_side.bdbexport.model.BulkExportRequest;
import de.bright_side.bdbexport.model.CatalogAndSchema;
import de.bright_side.bdbexport.model.InternalObjectExportRequest;
import de.bright_side.bdbexport.model.ObjectExportRequest;
import de.bright_side.bdbexport.model.ObjectNameFilter;

public class DbExportUtil {
	public static <K> boolean in(K item, @SuppressWarnings("unchecked") K ... items){
		for (K i: items) {
			if (item.equals(i)) {
				return true;
			}
		}
		return false;
	}

	public static void write(OutputStream output, String string) throws Exception{
		output.write(string.getBytes(DbExporter.CHARSET));
	}
	
	public static InternalObjectExportRequest toInternalRequest(ObjectExportRequest exportRequest) throws Exception {
		InternalObjectExportRequest result = new InternalObjectExportRequest();
		
		Connection connection = exportRequest.getConnection();
		result.setConnection(connection);
		result.setDbType(DbUtil.determineDbType(exportRequest.getConnection()));
		
		if (noValue(exportRequest.getUserName())){
			result.setUserName(DbUtil.determineUserName(connection));
		}
		
		result.setSortTableData(exportRequest.isSortTableData());
		result.setObjectType(exportRequest.getObjectType());
		result.setObjectName(exportRequest.getObjectName());

		if (!noValue(exportRequest.getWhereClause())) {
			result.setWhereClause(exportRequest.getWhereClause());
		}
		
		if (exportRequest.getTargetDbType() != null) {
			result.setTargetDbType(exportRequest.getTargetDbType());
		} else {
			result.setTargetDbType(result.getDbType());
		}
		
		if (exportRequest.getClobExportMaxLength() >= 0) {
			result.setClobExportMaxLength(exportRequest.getClobExportMaxLength());
		} else {
			result.setClobExportMaxLength(DbExporter.DEFAULT_CLOB_EXPORT_MAX_LENGTH);
		}
		
		CatalogAndSchema catalogAndSchema = DbUtil.replaceEmptyWithDefaults(connection, new CatalogAndSchema(exportRequest.getCatalog(), exportRequest.getSchema()));
		result.setCatalogAndSchema(catalogAndSchema);
		
		return result;
	}

	public static boolean matches(BulkExportRequest exportRequest, ObjectType objectType, String objectName) throws Exception {
		ObjectNameFilter filter = null;
		
		switch (objectType) {
		case TABLE_DDL:
			filter = exportRequest.getTableDdlFilter();
			break;
		case TABLE_DATA:
			filter = exportRequest.getTableDataFilter();
			break;
		case VIEW_DDL:
			filter = exportRequest.getViewDdlFilter();
			break;
		default:
			break;
		}
		
		if (filter == null) {
			filter = new ObjectNameFilter();
		}
		filter.setInclude(toUpperCase(filter.getInclude()));
		filter.setExclude(toUpperCase(filter.getExclude()));
		
		Set<String> includeFilter = filter.getInclude();
		Set<String> excludeFilter = filter.getExclude();
		
		if ((includeFilter == null) && (excludeFilter == null)) {
			//: no filter, allow all
			return true;
		}
		String useObjectName = objectName.toUpperCase();
		if ((excludeFilter != null) && (excludeFilter.contains(useObjectName))){
			//: it was specified explicitly that this item should be excluded
			return false;
		}
		if (includeFilter == null) {
			//: all items should be included
			return true;
		}
		
		//: check if the item was explicitly included
		return includeFilter.contains(useObjectName);
	}

	public static String collectionToString(Collection<?> collection, String itemPrefix, String itemSuffix, String itemSeparator) {
		StringBuffer sb = new StringBuffer("");
		int size = collection.size();
		int pos = 0;
		
		Iterator<?> iterator = collection.iterator();
		while(iterator.hasNext()){
			sb.append(itemPrefix);
			sb.append(iterator.next());
			sb.append(itemSuffix);
			pos ++;
			if (pos < size) sb.append(itemSeparator);
		}
		
		return sb.toString();
	}
	
	public static String collectionToString(Collection<?> collection, String itemSeparator) {
		return collectionToString(collection, "", "",  itemSeparator);
	}

	public static boolean noValue(String string) {
		return string == null || string.isEmpty();
	}

	public static Set<String> toUpperCase(Set<String> set) {
		if (set == null) {
			return null;
		}
		Set<String> result = new TreeSet<String>();
		for (String i: set) {
			result.add(i.toUpperCase());
		}
		return result;
	}

}
