package de.bright_side.bdbexport.bl;

public class DbExportTableScripts {
	public static final String TABLE_NAME_PLACEHOLDER = "${TABLE_NAME}";
	public static final String CATALOG_NAME_PLACEHOLDER = "${CATALOG_NAME}";
	public static final String SCHEMA_NAME_PLACEHOLDER = "${SCHEMA_NAME}";
	
	//: https://stackoverflow.com/questions/21547/in-sql-server-how-do-i-generate-a-create-table-statement-for-a-given-table
	public static String getMsSqlServerCreateTableDdlScript() {
		StringBuilder sb = new StringBuilder();
		
		sb.append("SELECT\n");
		sb.append("       'CREATE TABLE [' + obj.name + '] (' + LEFT(cols.list, LEN(cols.list) - 1 ) + ');'\n");
		sb.append("        + ISNULL(' ' + refs.list, '')\n");
		sb.append("    FROM [${CATALOG_NAME}].sys.sysobjects obj\n");
		sb.append("    CROSS APPLY (\n");
		sb.append("        SELECT \n");
		sb.append("            CHAR(10)\n");
		sb.append("            + ' [' + column_name + '] '\n");
		sb.append("            + data_type\n");
		sb.append("            + CASE data_type\n");
		sb.append("                WHEN 'sql_variant' THEN ''\n");
		sb.append("                WHEN 'text' THEN ''\n");
		sb.append("                WHEN 'ntext' THEN ''\n");
		sb.append("                WHEN 'xml' THEN ''\n");
		sb.append("                WHEN 'decimal' THEN '(' + CAST(numeric_precision as VARCHAR) + ', ' + CAST(numeric_scale as VARCHAR) + ')'\n");
		sb.append("                ELSE COALESCE('(' + CASE WHEN character_maximum_length = -1 THEN 'MAX' ELSE CAST(character_maximum_length as VARCHAR) END + ')', '')\n");
		sb.append("            END\n");
		sb.append("            + ' '\n");
		sb.append("            + case when exists ( -- Identity skip\n");
		sb.append("            select id from syscolumns\n");
		sb.append("            where object_name(id) = obj.name\n");
		sb.append("            and name = column_name\n");
		sb.append("            and columnproperty(id,name,'IsIdentity') = 1 \n");
		sb.append("            ) then\n");
		sb.append("            'IDENTITY(' + \n");
		sb.append("            cast(ident_seed(obj.name) as varchar) + ',' + \n");
		sb.append("            cast(ident_incr(obj.name) as varchar) + ')'\n");
		sb.append("            else ''\n");
		sb.append("            end + ' '\n");
		sb.append("            + CASE WHEN IS_NULLABLE = 'No' THEN 'NOT ' ELSE '' END\n");
		sb.append("            + 'NULL'\n");
		sb.append("            + CASE WHEN information_schema.columns.column_default IS NOT NULL THEN ' DEFAULT ' + information_schema.columns.column_default ELSE '' END\n");
		sb.append("            + ','\n");
		sb.append("        FROM\n");
		sb.append("            [${CATALOG_NAME}].INFORMATION_SCHEMA.COLUMNS\n");
		sb.append("        WHERE table_name = obj.name\n");
		sb.append("        ORDER BY ordinal_position\n");
		sb.append("        FOR XML PATH('')\n");
		sb.append("    ) cols (list)\n");
		sb.append("    CROSS APPLY(\n");
		sb.append("        SELECT\n");
		sb.append("            CHAR(10) + 'ALTER TABLE ' + obj.name + ' ADD ' + LEFT(alt, LEN(alt)-1) + ';'\n");
		sb.append("        FROM(\n");
		sb.append("            SELECT\n");
		sb.append("                CHAR(10)\n");
		sb.append("                + ' CONSTRAINT ' + tc.constraint_name\n");
		sb.append("                + ' ' + tc.constraint_type + ' (' + LEFT(c.list, LEN(c.list)-1) + ')'\n");
		sb.append("                + COALESCE(CHAR(10) + r.list, ', ')\n");
		sb.append("            FROM\n");
		sb.append("                [${CATALOG_NAME}].information_schema.table_constraints tc\n");
		sb.append("                CROSS APPLY(\n");
		sb.append("                    SELECT\n");
		sb.append("                        '[' + kcu.column_name + '], '\n");
		sb.append("                    FROM\n");
		sb.append("                        [${CATALOG_NAME}].information_schema.key_column_usage kcu\n");
		sb.append("                    WHERE\n");
		sb.append("                        kcu.constraint_name = tc.constraint_name\n");
		sb.append("                    ORDER BY\n");
		sb.append("                        kcu.ordinal_position\n");
		sb.append("                    FOR XML PATH('')\n");
		sb.append("                ) c (list)\n");
		sb.append("                OUTER APPLY(\n");
		sb.append("                    SELECT\n");
		sb.append("                        '  REFERENCES [' + kcu1.constraint_schema + '].' + '[' + kcu2.table_name + ']' + '(' + kcu2.column_name + '), '\n");
		sb.append("                    FROM [${CATALOG_NAME}].information_schema.referential_constraints as rc\n");
		sb.append("                        JOIN [${CATALOG_NAME}].information_schema.key_column_usage as kcu1 ON (kcu1.constraint_catalog = rc.constraint_catalog AND kcu1.constraint_schema = rc.constraint_schema AND kcu1.constraint_name = rc.constraint_name)\n");
		sb.append("                        JOIN [${CATALOG_NAME}].information_schema.key_column_usage as kcu2 ON (kcu2.constraint_catalog = rc.unique_constraint_catalog AND kcu2.constraint_schema = rc.unique_constraint_schema AND kcu2.constraint_name = rc.unique_constraint_name AND kcu2.ordinal_position = KCU1.ordinal_position)\n");
		sb.append("                    WHERE\n");
		sb.append("                        kcu1.constraint_catalog = tc.constraint_catalog AND kcu1.constraint_schema = tc.constraint_schema AND kcu1.constraint_name = tc.constraint_name\n");
		sb.append("                ) r (list)\n");
		sb.append("            WHERE tc.table_name = obj.name\n");
		sb.append("            FOR XML PATH('')\n");
		sb.append("        ) a (alt)\n");
		sb.append("    ) refs (list)\n");
		sb.append("    WHERE\n");
		sb.append("        xtype = 'U'\n");
		sb.append("    AND name NOT IN ('dtproperties')\n");
		sb.append("    AND obj.name = '${TABLE_NAME}'\n");

		return sb.toString();
	}
	
	public static String getMsSqlServerTableDependenciesScript() {
		StringBuilder sb = new StringBuilder();
		sb.append("select kcu2.TABLE_NAME as table_name\n");
		sb.append("  from [${CATALOG_NAME}].INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc \n");
		sb.append("  left join [${CATALOG_NAME}].INFORMATION_SCHEMA.referential_constraints rc on tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME\n");
		sb.append("  left join [${CATALOG_NAME}].INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu1 on tc.CONSTRAINT_NAME = kcu1.CONSTRAINT_NAME \n");
		sb.append("  left join [${CATALOG_NAME}].INFORMATION_SCHEMA.key_column_usage as kcu2 ON (kcu2.constraint_name = rc.unique_constraint_name AND kcu2.ordinal_position = KCU1.ordinal_position)\n");
		sb.append(" where tc.CONSTRAINT_TYPE  = 'FOREIGN KEY'\n");
		sb.append("   and tc.CONSTRAINT_CATALOG = 'plb-db'\n");
		sb.append("   and rc.CONSTRAINT_CATALOG = 'plb-db'\n");
		sb.append("   and kcu1.CONSTRAINT_CATALOG = 'plb-db'\n");
		sb.append("   and kcu2.CONSTRAINT_CATALOG = 'plb-db'\n");
		sb.append("   and tc.CONSTRAINT_SCHEMA = '${SCHEMA_NAME}'\n");
		sb.append("   and rc.CONSTRAINT_SCHEMA = '${SCHEMA_NAME}'\n");
		sb.append("   and kcu1.CONSTRAINT_SCHEMA = '${SCHEMA_NAME}'\n");
		sb.append("   and kcu2.CONSTRAINT_SCHEMA = '${SCHEMA_NAME}'\n");
		sb.append("   and tc.TABLE_NAME = '${TABLE_NAME}'\n");
		return sb.toString();
	}
}
