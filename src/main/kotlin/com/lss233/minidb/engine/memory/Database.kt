package com.lss233.minidb.engine.memory

import com.lss233.minidb.engine.Cell
import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.expression.primary.Identifier
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
            Column("oid"), Column("datname"), Column("datdba"), Column("encoding"),
            Column("datlocprovider"), Column("datistemplate"), Column("datallowconn"),
            Column("datconnlimit"), Column("dattablespace"), Column("datcollate"),
            Column("datctype"), Column("datacl")
        ), mutableListOf())

        pgCatalogSchema["pg_namespace"] = Table("pg_namespace", mutableListOf(
                Column("oid"), Column("nspname"), Column("nspower"), Column("nspacl")
            ), mutableListOf(
                NTuple.from("pg_catalog", "pg_catalog", "10", "{postgres=UC/postgres,=U/postgres}"),
                NTuple.from("public", "public", "10", "{postgres=UC/postgres,=U/postgres}"),
            )
        )
        pgCatalogSchema["pg_tablespace"]  = Table("pg_tablespace", mutableListOf(
                Column("oid"), Column("spcname"), Column("spcowner"), Column("spcacl"), Column("spcoptions")
            ), mutableListOf(
                NTuple.from("1", "default_tablespace", "1", "", "")
            )
        )

        pgCatalogSchema["pg_settings"]  = Table("pg_settings", mutableListOf(
                Column("set_config('bytea_output','hex',false)"), Column("name")
            ), mutableListOf(
                NTuple.from("1", "bytea_output")
            )
        )
        pgCatalogSchema["pg_type"]  = Table("pg_type", mutableListOf(
                Column("oid"), Column("typname"), Column("typnamespace"), Column("typowner"),
                Column("typlen"), Column("typbyval"), Column("typtype"), Column("typcategory"), Column("typdelim"),
                Column("typndims")
            ), mutableListOf()
        )
        pgCatalogSchema["pg_class"]  = Table("pg_class", mutableListOf(
                Column("oid"), Column("relname"), Column("relnamespace"), Column("reltype"),
                Column("reloftype"), Column("relowner"), Column("relam"), Column("relfilenode"), Column("reltablespace"),
                Column("relkind")
            ), mutableListOf()
        )

        pgCatalogSchema["pg_attribute"]  = Table("pg_attribute", mutableListOf(
                Column("attrelid"), Column("attname"), Column("atttypid"), Column("attstattarget"),
                Column("attlen"), Column("attnum"), Column("attndims"), Column("attcacheoff"), Column("attbyval")
            ), mutableListOf(
                NTuple.from("1", "bytea_output")
            )
        )
        pgCatalogSchema["pg_inherits"] = Table("pg_inherits", mutableListOf(
                Column("inhrelid"), Column("inhparent"), Column("inhseqno")
        ), mutableListOf())

        pgCatalogSchema["pg_index"] = Table("pg_index", mutableListOf(
            Column("indexrelid"), Column("indrelid"), Column("indnatts")
        ), mutableListOf())

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
            Column("specific_schema"), Column("specific_name"), Column("routine_catalog"), Column("routine_schema"),
            Column("routine_name"), Column("routine_type"), Column("module_catalog"), Column("data_type")
        ), mutableListOf())
        informationSchema["parameters"] = Table("parameters", mutableListOf(
            Column("specific_schema"), Column("specific_name"), Column("routine_catalog"), Column("ordinal_position"),
            Column("parameter_mode"), Column("is_result"), Column("as_locator"), Column("parameter_name"), Column("data_type")
        ), mutableListOf())

        informationSchema["tables"] = Table("tables", mutableListOf(), mutableListOf())
        informationSchema["columns"] = Table("columns", mutableListOf(
            Column("table_catalog"), Column("table_schema"), Column("table_name"), Column("column_name"), Column("ordinal_position"),
            Column("udt_catalog"), Column("udt_schema"), Column("udt_name")
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

}