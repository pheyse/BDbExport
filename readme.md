# BDBExport
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
 
## Description
A java framework to export database data and DDLs - independent of the database vendor. 

## Stability
The current version is < 1.0.0 so not all features are complete and many features haven't been tested yet. Even in this version it may be very useful for development purposes.
 
## Features
 - Database types: H2, MS SQL Server, MySQL, MariaDB, SQLite
 - Export types: DDL for tables, DDL for views, table data
 - Where-clause for table data export
 - Filtering by object names
 - Sorting exported table data
 - Automatic detection of database type, schema and catalog based on provided JDBC connection

## Usage
### Including via Maven
```xml
[...]
	<dependency>
		<groupId>de.bright-side.bdbexport</groupId>
		<artifactId>BDBExport</artifactId>
		<version>0.2.0</version>
	</dependency>
[...]
```

### Including via Gradle
```
dependencies {
    implementation 'de.bright-side.dbexport:BDBExport:0.2.0'
}
```

### Exporting database examples
#### Exporting all data and objects as a string
```java
BulkExportRequest exportRequest = new BulkExportRequest();
exportRequest.setConnection(connection);
String script = new BDbExporter().bulkExportAsString(exportRequest);
```


#### Export all data and objects to stream
```java
BulkExportRequest exportRequest = new BulkExportRequest();
exportRequest.setConnection(connection);
new BDbExporter().bulkExport(exportRequest, null, myOutputStream);
```

#### Detailed configuration
```java
BulkExportRequest exportRequest = new BulkExportRequest();
exportRequest.setConnection(connection);

exportRequest.setObjectTypes(new HashSet<ObjectType>(Arrays.asList(ObjectType.TABLE_DDL, ObjectType.TABLE_DATA)));
exportRequest.setSortTableData(true);

ObjectNameFilter tableDataFilter = new ObjectNameFilter();
tableDataFilter.setExclude(Collections.singleton("LOG"));
exportRequest.setTableDataFilter(tableDataFilter);

new BDbExporter().bulkExport(exportRequest, null, response.getOutputStream());
```

#### Spring Boot service with Hibernate as JPA provider
```java

[...]
@Controller
@Transactional
public class MainController{
	private final Logger LOGGER = LoggerFactory.getLogger(MainController.class);

	@GetMapping("/export-db")
	public void exportDb(final HttpServletResponse response) throws Exception {
		Session session = em.unwrap(Session.class);
		final ReturnableValue<Exception> exception = new ReturnableValue<Exception>(null);
		
		session.doWork(new Work() {
			@Override
			public void execute(Connection connection) throws SQLException {
				BulkExportRequest exportRequest = new BulkExportRequest();
				exportRequest.setConnection(connection);
				
				try {
					new BDbExporter().bulkExport(exportRequest, null, response.getOutputStream());
				} catch (Exception e) {
					exception.setValue(e);
					LOGGER.error("Could not export database", e);
				}
			}
		});
		if (exception.getValue() != null) {
			throw exception.getValue();
		}
		response.flushBuffer();
	}

```



## Change History
 - Version 0.1.0 (2020-07-20)
    - initial version
 - Version 0.1.1 (2020-07-31)
    - smaller bugfix: additional MS SQL Server data types