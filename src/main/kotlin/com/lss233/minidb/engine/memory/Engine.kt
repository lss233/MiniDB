package com.lss233.minidb.engine.memory

import com.lss233.minidb.engine.Cell
import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.SQLParser
import com.lss233.minidb.engine.schema.Column
import com.lss233.minidb.engine.storage.StorageService
import com.lss233.minidb.engine.visitor.CreateTableStatementVisitor
import com.lss233.minidb.networking.Session
import hu.webarticum.treeprinter.printer.traditional.TraditionalTreePrinter
import miniDB.parser.ast.expression.primary.Identifier
import miniDB.parser.ast.stmt.ddl.DDLCreateTableStatement
import kotlin.collections.HashMap

object Engine {
    val systemSession: Session = Session()
    private val databases = HashMap<String, Database>()
    val session = ThreadLocal<Session>()

    private val storageService = StorageService()

    init {
        systemSession.properties["database"] = "minidb"
    }

    fun execute(sql: String): Any? {
        when(val ast = SQLParser.parse(sql)) {
            is DDLCreateTableStatement -> {
                val visitor = CreateTableStatementVisitor()
                try {
                    ast.accept(visitor)
                    databases[session.get()?.properties?.get("database") ?: "minidb"]?.
                        createTable(visitor.relation!!, visitor.tableIdentifier!!)
                            ?: throw RuntimeException("Database not exists.")
                } finally {
                    TraditionalTreePrinter().print(visitor.rootNode)
                }
            }
        }
        return null
    }
    operator fun get(identifier: Identifier) : Table {
        val db = databases[session.get()?.properties?.get("database") ?: "minidb"] ?: throw RuntimeException("Database not exists.")
        val schema = if(identifier.parent == null) {
            db["pg_catalog"]
        } else {
            db[identifier.parent.idText]
        }
        return schema[identifier.idText]
    }

    operator fun get(dbName: String): Database {
        if(!databases.containsKey(dbName)) {
            throw RuntimeException("Database $dbName does not exist.")
        }
        return databases[dbName]!!
    }

    fun createDatabase(name: String, dba: Int = 10, encoding: Int = 1, locProvider: Char = 'c', allowConn: Boolean = true, connLimit: Int = -1): Database {
        if(databases.containsKey(name)) {
            throw RuntimeException("Database $name already exists.")
        }
        val db = Database(name, dba, encoding, locProvider, allowConn, connLimit)

        databases[name] = db
        // Assign system schema
        db.initSchema()

        // Create db schema
        db.createSchema("public")

        registerDatabase(db)
        return db
    }

    private fun registerDatabase(db: Database) {
        for(item in databases.values) {
            item["pg_catalog"]["pg_database"].insert(
                NTuple.from(
                    Cell(Column("oid"), "3"),
                    Cell(Column("datname"), db.name),
                    Cell(Column("datdba"), "1"),
                    Cell(Column("encoding"), "1"),
                    Cell(Column("datlocprovider"), 'c'),
                    Cell(Column("datistemplate"), "true"),
                    Cell(Column("datallowconn"), "true"),
                    Cell(Column("datconnlimit"), -1),
                    Cell(Column("dattablespace"), "1"),
                    Cell(Column("datcollate"), "1"),
                    Cell(Column("datctype"), "1"),
                    Cell(Column("datacl"), "[]")
                )
            )
        }
    }

    fun loadStorageData() {
        val databaseList = storageService.getDatabaseList()
        if (databaseList.isEmpty()) {
            // TODO init storage service
        }
        // get the databaseList
        for (database in databaseList) {

            val tempDatabase = Database(database,10,1,'c',true,-1)
            // base on the database name to get the schemas
            val schemaList = storageService.getSchemaList(database)
            for (schema in schemaList) {
                val tempSchema = Schema(schemaName = schema)
                // base on the schema name to get the tables
                val tableList = storageService.getTableList(database, schema)
                for (table in tableList) {
                    tempSchema[table] = storageService.getTable(dbName = database, tableName = table, schemaName = schema)
                }
                tempDatabase[schema] = tempSchema
            }
            this.databases[database] = tempDatabase
        }
    }

    // is temp
    fun dataStorage() {
        // When the system data table has been created
        for (database in this.databases) {
            // create database
            for (schema in database.value.schemas) {
                // TODO should be change
                if (schema.key == "pg_catalog" || schema.key == "information_schema") {
                    continue
                }
                // mark the schema to the table
                for (table in schema.value.tables) {
                    // mark the table
                    println("aaaaaaaaa " + table.value.columns[0].name)
                    storageService.updateOrSaveTable(tableName = table.key, dbName = database.key, schemaName = schema.key, table = table.value)
                }
            }
        }
    }
}
