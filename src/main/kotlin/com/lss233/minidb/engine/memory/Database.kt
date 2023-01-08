package com.lss233.minidb.engine.memory

import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.expression.primary.Identifier
import miniDB.parser.ast.fragment.ddl.datatype.DataType
import kotlin.collections.HashMap
import kotlin.concurrent.getOrSet

class Database(val name: String, val dba: Int, val encoding: Int, val locProvider: Char, val allowConn: Boolean, val connLimit: Int) {
    var schemas = HashMap<String, Schema>();

    fun createTable(table: Table, identifier: Identifier): Database {
        val schema = schemas[identifier.parent.idText] ?: this["pg_catalog"]

        if(schema.tables.containsKey(table.name)) {
            throw RuntimeException("Table ${table.name} already exists.")
        }

        schema[table.name] = table
        // TODO insert table info
        this["pg_catalog"]["pg_class"].let {
            run {
                it.insert(arrayOf(1, table.name, identifier.parent.idText, 0, 0, 10, 0, "mem", "1", "r"))
            } }
        this["information_schema"]["columns"].let {
            run {
                table.columns.forEachIndexed { index, col -> run {
                    it.insert(arrayOf(
                        "minidb",
                        identifier.parent.idText,
                        identifier.idText,
                        col.identifier.idText,
                        index,
                        "minidb",
                        identifier.parent.idText,
                        col.definition.dataType.typeName.name
                    ))
                } }

            }
        }
        return this
    }
//    operator fun set(tableName: String, table: Table) {
//        tables[tableName] = table
//    }

    operator fun get(schemaName: String): Schema {
        return schemas[schemaName] ?: throw RuntimeException("Schema $schemaName does not exists.");
    }
    operator fun set(schemaName: String, schema: Schema) {
        schemas[schemaName] = schema
    }

    fun createSchema(schemaName: String): Schema {
        val schema = Schema(schemaName)
        this[schemaName] = schema
        return schema
    }

    fun initSchema() {
        val pgCatalogSchema = createSchema("pg_catalog")

        pgCatalogSchema["pg_database"] = Table("pg_database", mutableListOf(
            Column("oid", DataType.DataTypeName.INT), Column("datname", DataType.DataTypeName.CHAR), Column("datdba", DataType.DataTypeName.CHAR),
            Column("encoding", DataType.DataTypeName.CHAR), Column("datlocprovider", DataType.DataTypeName.CHAR),
            Column("datistemplate", DataType.DataTypeName.CHAR), Column("datallowconn", DataType.DataTypeName.CHAR),
            Column("datconnlimit", DataType.DataTypeName.CHAR), Column("dattablespace", DataType.DataTypeName.CHAR),
            Column("datcollate", DataType.DataTypeName.CHAR), Column("datctype", DataType.DataTypeName.CHAR),
            Column("datacl", DataType.DataTypeName.CHAR)
        ), mutableListOf())

        pgCatalogSchema["pg_namespace"] = Table("pg_namespace", mutableListOf(
                Column("oid", DataType.DataTypeName.INT), Column("nspname", DataType.DataTypeName.CHAR),
                Column("nspower", DataType.DataTypeName.CHAR), Column("nspacl", DataType.DataTypeName.CHAR)
            ), mutableListOf(
                NTuple.from("pg_catalog", "pg_catalog", "10", "{postgres=UC/postgres,=U/postgres}"),
                NTuple.from("public", "public", "10", "{postgres=UC/postgres,=U/postgres}"),
            )
        )
        pgCatalogSchema["pg_tablespace"]  = Table("pg_tablespace", mutableListOf(
                Column("oid", DataType.DataTypeName.INT), Column("spcname", DataType.DataTypeName.CHAR),
                Column("spcowner", DataType.DataTypeName.CHAR), Column("spcacl", DataType.DataTypeName.CHAR),
                Column("spcoptions", DataType.DataTypeName.CHAR)
            ), mutableListOf(
                NTuple.from("1", "default_tablespace", "1", "", "")
            )
        )

        pgCatalogSchema["pg_settings"]  = Table("pg_settings", mutableListOf(
                Column("set_config('bytea_output','hex',false)", DataType.DataTypeName.CHAR), Column("name", DataType.DataTypeName.CHAR)
            ), mutableListOf(
                NTuple.from("1", "bytea_output")
            )
        )
        pgCatalogSchema["pg_type"]  = Table("pg_type", mutableListOf(
                Column("oid", DataType.DataTypeName.INT), Column("typname", DataType.DataTypeName.CHAR),
                Column("typnamespace", DataType.DataTypeName.CHAR), Column("typowner", DataType.DataTypeName.CHAR),
                Column("typlen", DataType.DataTypeName.CHAR), Column("typbyval", DataType.DataTypeName.CHAR),
                Column("typtype", DataType.DataTypeName.CHAR), Column("typcategory", DataType.DataTypeName.CHAR),
                Column("typdelim", DataType.DataTypeName.CHAR), Column("typndims", DataType.DataTypeName.CHAR)
            ), mutableListOf()
        )
        pgCatalogSchema["pg_class"]  = Table("pg_class", mutableListOf(
                Column("oid", DataType.DataTypeName.INT), Column("relname", DataType.DataTypeName.CHAR),
                Column("relnamespace", DataType.DataTypeName.CHAR), Column("reltype", DataType.DataTypeName.CHAR),
                Column("reloftype", DataType.DataTypeName.CHAR), Column("relowner", DataType.DataTypeName.CHAR),
                Column("relam", DataType.DataTypeName.CHAR), Column("relfilenode", DataType.DataTypeName.CHAR),
                Column("reltablespace", DataType.DataTypeName.CHAR), Column("relkind", DataType.DataTypeName.CHAR)
            ), mutableListOf()
        )

        pgCatalogSchema["pg_attribute"]  = Table("pg_attribute", mutableListOf(
                Column("attrelid", DataType.DataTypeName.INT), Column("attname", DataType.DataTypeName.CHAR),
                Column("atttypid", DataType.DataTypeName.INT), Column("attstattarget", DataType.DataTypeName.CHAR),
                Column("attlen", DataType.DataTypeName.CHAR), Column("attnum", DataType.DataTypeName.CHAR),
                Column("attndims", DataType.DataTypeName.CHAR), Column("attcacheoff", DataType.DataTypeName.CHAR),
                Column("attbyval", DataType.DataTypeName.CHAR)
            ), mutableListOf(
                NTuple.from("1", "bytea_output")
            )
        )
        pgCatalogSchema["pg_inherits"] = Table("pg_inherits", mutableListOf(
                Column("inhrelid", DataType.DataTypeName.INT), Column("inhparent", DataType.DataTypeName.CHAR),
                Column("inhseqno", DataType.DataTypeName.CHAR)
        ), mutableListOf())

        pgCatalogSchema["pg_index"] = Table("pg_index", mutableListOf(
            Column("indexrelid", DataType.DataTypeName.INT), Column("indrelid", DataType.DataTypeName.INT),
            Column("indnatts", DataType.DataTypeName.CHAR)
        ), mutableListOf())

        // TODO other task
        pgCatalogSchema["pg_foreign_table"] = Table("pg_foreign_table", mutableListOf(), mutableListOf())
        pgCatalogSchema["pg_foreign_server"] = Table("pg_foreign_server", mutableListOf(), mutableListOf())
        pgCatalogSchema["pg_roles"] = Table("pg_roles", mutableListOf(), mutableListOf())
        pgCatalogSchema["pg_attrdef"] = Table("pg_attrdef", mutableListOf(), mutableListOf())
        pgCatalogSchema["pg_attribute"] = Table("pg_attribute", mutableListOf(), mutableListOf())
        pgCatalogSchema["pg_am"] = Table("pg_am", mutableListOf(), mutableListOf())
        pgCatalogSchema["pg_operator"] = Table("pg_operator", mutableListOf(), mutableListOf())
        pgCatalogSchema["pg_depend"] = Table("pg_depend", mutableListOf(), mutableListOf())
        pgCatalogSchema["pg_matviews"] = Table("pg_matviews", mutableListOf(), mutableListOf())


        val informationSchema = createSchema("information_schema")
        informationSchema["routines"] = Table("routines", mutableListOf(
            Column("specific_schema", DataType.DataTypeName.CHAR), Column("specific_name", DataType.DataTypeName.CHAR),
            Column("routine_catalog", DataType.DataTypeName.CHAR), Column("routine_schema", DataType.DataTypeName.CHAR),
            Column("routine_name", DataType.DataTypeName.CHAR), Column("routine_type", DataType.DataTypeName.CHAR),
            Column("module_catalog", DataType.DataTypeName.CHAR), Column("data_type", DataType.DataTypeName.CHAR)
        ), mutableListOf())
        informationSchema["parameters"] = Table("parameters", mutableListOf(
            Column("specific_schema", DataType.DataTypeName.CHAR), Column("specific_name", DataType.DataTypeName.CHAR),
            Column("routine_catalog", DataType.DataTypeName.CHAR), Column("ordinal_position", DataType.DataTypeName.CHAR),
            Column("parameter_mode", DataType.DataTypeName.CHAR), Column("is_result", DataType.DataTypeName.CHAR),
            Column("as_locator", DataType.DataTypeName.CHAR), Column("parameter_name", DataType.DataTypeName.CHAR),
            Column("data_type", DataType.DataTypeName.CHAR)
        ), mutableListOf())

        informationSchema["tables"] = Table("tables", mutableListOf(), mutableListOf())
        informationSchema["columns"] = Table("columns", mutableListOf(
            Column("table_catalog", DataType.DataTypeName.CHAR), Column("table_schema", DataType.DataTypeName.CHAR),
            Column("table_name", DataType.DataTypeName.CHAR), Column("column_name", DataType.DataTypeName.CHAR),
            Column("ordinal_position", DataType.DataTypeName.CHAR), Column("udt_catalog", DataType.DataTypeName.CHAR),
            Column("udt_schema", DataType.DataTypeName.CHAR), Column("udt_name", DataType.DataTypeName.CHAR)
        ), mutableListOf())

        // 以用户身份执行以下建表语句
        // 为了保证线程安全，先保存之前的会话信息
        // 若当前上下文没有会话信息，则使用系统会话
        val oldSession = Engine.session.getOrSet { Engine.systemSession }
        Engine.execute("CREATE TABLE pg_catalog.pg_collation (\n" +
                "    oid integer NOT NULL,\n" +
                "    collname text NOT NULL,\n" +
                "    collnamespace integer NOT NULL,\n" +
                "    collowner integer NOT NULL,\n" +
                "    collprovider text NOT NULL,\n" +
                "    collisdeterministic boolean NOT NULL,\n" +
                "    collencoding integer NOT NULL,\n" +
                "    collcollate text NOT NULL,\n" +
                "    collctype text NOT NULL,\n" +
                "    collversion text\n" +
                ")")
        Engine.execute("CREATE TABLE pg_catalog.pg_constraint (\n" +
                "    oid integer NOT NULL,\n" +
                "    conname text NOT NULL,\n" +
                "    connamespace integer NOT NULL,\n" +
                "    contype text NOT NULL,\n" +
                "    condeferrable tinyint NOT NULL,\n" +
                "    condeferred tinyint NOT NULL,\n" +
                "    convalidated tinyint NOT NULL,\n" +
                "    conrelid integer NOT NULL,\n" +
                "    contypid integer NOT NULL,\n" +
                "    conindid integer NOT NULL,\n" +
                "    conparentid integer NOT NULL,\n" +
                "    confrelid integer NOT NULL,\n" +
                "    confupdtype text NOT NULL,\n" +
                "    confdeltype text NOT NULL,\n" +
                "    confmatchtype text NOT NULL,\n" +
                "    conislocal boolean NOT NULL,\n" +
                "    coninhcount integer NOT NULL,\n" +
                "    connoinherit boolean NOT NULL\n" +
                ")")
        Engine.execute("CREATE TABLE pg_catalog.pg_opclass (\n" +
                "    oid integer NOT NULL,\n" +
                "    opcmethod integer NOT NULL,\n" +
                "    opcname text NOT NULL,\n" +
                "    opcnamespace integer NOT NULL,\n" +
                "    opcowner integer NOT NULL,\n" +
                "    opcfamily integer NOT NULL,\n" +
                "    opcintype integer NOT NULL,\n" +
                "    opcdefault boolean NOT NULL,\n" +
                "    opckeytype integer NOT NULL\n" +
                ")")
        Engine.session.set(oldSession)

    }

    fun dropTable(identifier: Identifier) {
        val schema = schemas[identifier.parent.idText] ?: this["pg_catalog"]

        if(!schema.tables.containsKey(identifier.idText)) {
            throw RuntimeException("Table ${identifier.idText} not exist.")
        }
        schema.tables.remove(identifier.idText)
    }

}
