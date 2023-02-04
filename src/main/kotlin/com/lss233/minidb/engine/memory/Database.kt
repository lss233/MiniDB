package com.lss233.minidb.engine.memory

import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.config.MiniDBConfig
import com.lss233.minidb.engine.memory.internal.catalog.*
import com.lss233.minidb.engine.memory.internal.information.ColumnsView
import com.lss233.minidb.engine.memory.internal.information.ParametersView
import com.lss233.minidb.engine.memory.internal.information.RoutinesView
import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.expression.primary.Identifier
import miniDB.parser.ast.fragment.ddl.datatype.DataType
import java.nio.file.Paths
import kotlin.concurrent.getOrSet

class Database(val name: String, val dba: Int, val encoding: Int, val locProvider: Char, val allowConn: Boolean, val connLimit: Int)
    : HashMap<String, Schema>() {
    var schemas = HashMap<String, Schema>()

    private var absFilePath: String = Paths.get(MiniDBConfig.DATA_FILE, name).toString()

    fun createTable(table: Table, identifier: Identifier): Database {

        val schema: Schema = if (!this.containsKey(identifier.parent.idText)) this["pg_catalog"]
        else this[identifier.parent.idText]

        if(schema.containsKey(table.tableName)) {
            throw RuntimeException("View or Table with name ${table.tableName} already exists.")
        }

        schema[table.tableName] = table
//        this["information_schema"]["columns"].let {
//            run {
//                table.columns.forEachIndexed { index, col -> run {
//                    it.insert(arrayOf(
//                        "minidb",
//                        identifier.parent.idText,
//                        identifier.idText,
//                        col.identifier.idText,
//                        index,
//                        "minidb",
//                        identifier.parent.idText,
//                        col.definition.dataType.typeName.name
//                    ))
//                } }
//
//            }
//        }
        return this
    }
//    operator fun set(tableName: String, table: Table) {
//        tables[tableName] = table
//    }

    override operator fun get(key: String): Schema {
        if (!this.containsKey(key)) {
            throw RuntimeException("Schema $key does not exists.")
        }
        return this[key]
    }

    /**
     * The privatization method avoids unnecessary set calls.
     */
    private operator fun set(schemaName: String, schema: Schema) {
        this[schemaName] = schema
    }

    fun createSchema(schemaName: String): Schema {
        val schema = Schema(schemaName, this.name)
        this[schemaName] = schema
        return schema
    }

    fun initSchema() {
        val pgCatalogSchema = createSchema("pg_catalog")
        pgCatalogSchema["pg_database"] = DatabaseView(this)
        pgCatalogSchema["pg_namespace"] = NamespaceView(this)
        pgCatalogSchema["pg_tablespace"]  = TablespaceView(this)
        pgCatalogSchema["pg_type"]  = TypeView(this)

        // skip
        pgCatalogSchema["pg_settings"]  = Table("pg_settings", mutableListOf(
                Column("set_config('bytea_output','hex',false)", DataType.DataTypeName.CHAR), Column("name", DataType.DataTypeName.CHAR)
            ), mutableListOf(
                NTuple.from("1", "bytea_output")
            ), this.name, "pg_settings"
        )

        pgCatalogSchema["pg_inherits"] = InheritsView(this)
        pgCatalogSchema["pg_class"]  = ClassView(this)
        pgCatalogSchema["pg_attribute"]  = AttributeView(this)
        pgCatalogSchema["pg_index"] = IndexView(this)
        pgCatalogSchema["pg_opclass"] = OpclassView(this)
        pgCatalogSchema["pg_rewrite"] = RewriteView(this)


        pgCatalogSchema["pg_foreign_table"] = Table("pg_foreign_table",
            mutableListOf(), mutableListOf(), this.name, "pg_catalog")
        pgCatalogSchema["pg_foreign_server"] = Table("pg_foreign_server",
            mutableListOf(), mutableListOf(), this.name, "pg_catalog")
        pgCatalogSchema["pg_roles"] = Table("pg_roles",
            mutableListOf(), mutableListOf(), this.name, "pg_catalog")
        pgCatalogSchema["pg_attrdef"] = Table("pg_attrdef",
            mutableListOf(), mutableListOf(), this.name, "pg_catalog")
        pgCatalogSchema["pg_attribute"] = Table("pg_attribute",
            mutableListOf(), mutableListOf(), this.name, "pg_catalog")
        pgCatalogSchema["pg_am"] = Table("pg_am",
            mutableListOf(), mutableListOf(), this.name, "pg_catalog")
        pgCatalogSchema["pg_operator"] = Table("pg_operator",
            mutableListOf(), mutableListOf(), this.name,  "pg_catalog")
        pgCatalogSchema["pg_depend"] = Table("pg_depend",
            mutableListOf(), mutableListOf(), this.name, "pg_catalog")
        pgCatalogSchema["pg_matviews"] = Table("pg_matviews",
            mutableListOf(), mutableListOf(), this.name, "pg_catalog")


        val informationSchema = createSchema("information_schema")
        informationSchema["routines"] = RoutinesView()
        informationSchema["parameters"] = ParametersView()

        informationSchema["tables"] = Table("tables",
            mutableListOf(), mutableListOf(), this.name, "information_schema")
        informationSchema["columns"] = ColumnsView()

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
        Engine.session.set(oldSession)

    }

    fun dropTable(identifier: Identifier) {
        val schema = if (!this.containsKey(identifier.parent.idText)) this["pg_catalog"]
        else this[identifier.parent.idText]
        if(!schema.containsKey(identifier.idText)) {
            throw RuntimeException("View or Table with name ${identifier.idText} does not exist.")
        }
        schema.remove(identifier.idText)
    }

}
