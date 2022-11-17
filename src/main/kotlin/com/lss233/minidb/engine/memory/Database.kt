package com.lss233.minidb.engine.memory

import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.schema.Column
import kotlin.collections.HashMap

class Database(val name: String, val dba: Int, val encoding: Int, val locProvider: Char, val allowConn: Boolean, val connLimit: Int) {
    var tables = HashMap<String, Table>();
    var schemas = HashMap<String, Schema>();

    fun createTable(table: Table): Database {
        if(tables.containsKey(table.name)) {
            throw RuntimeException("Table ${table.name} already exists.")
        }
        tables[table.name] = table
        // TODO insert table info
        return this
    }
    operator fun set(tableName: String, table: Table) {
        tables[tableName] = table
    }

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
            NTuple.from("1", "pg_catalog", "10", "{postgres=UC/postgres,=U/postgres}"),
            NTuple.from("2", "public", "10", "{postgres=UC/postgres,=U/postgres}"),
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


        val informationSchema = createSchema("information_schema")
        informationSchema["routines"] = Table("routines", mutableListOf(
            Column("specific_schema"), Column("specific_name"), Column("routine_catalog"), Column("routine_schema"),
            Column("routine_name"), Column("routine_type"), Column("module_catalog"), Column("data_type")
        ), mutableListOf())
        informationSchema["parameters"] = Table("parameters", mutableListOf(
            Column("specific_schema"), Column("specific_name"), Column("routine_catalog"), Column("ordinal_position"),
            Column("parameter_mode"), Column("is_result"), Column("as_locator"), Column("parameter_name"), Column("data_type")
        ), mutableListOf())

    }

}