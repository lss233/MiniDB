package com.lss233.minidb.engine.storage

import com.lss233.minidb.engine.storage.Schema
import com.lss233.minidb.engine.config.MiniDBConfig
import com.lss233.minidb.engine.memory.Table
import com.lss233.minidb.exception.MiniDBException
import com.lss233.minidb.utils.Misc
import miniDB.parser.ast.expression.primary.Identifier
import java.io.File
import java.io.IOException
import java.nio.file.Paths


class Database {

    private val databaseName:String

    // the schemas in the database
    private var schemas = HashMap<String, Schema>()

    private var absFilePath: String

    constructor(name: String) {
        this.databaseName = name
        absFilePath = Paths.get(MiniDBConfig.DATA_FILE, databaseName).toString()
    }

    constructor() {
        this.databaseName = "defaultDatabase"
        absFilePath = Paths.get(MiniDBConfig.DATA_FILE, databaseName).toString()
    }

    @Throws(MiniDBException::class)
    fun create() {
        val file = File(absFilePath)
        if (file.exists()) throw MiniDBException(String.format("Database %s already exists!", databaseName))
        if (!file.mkdir()) throw MiniDBException(
            String.format(
                "Failed to create the database %s. Cannot create directory.",
                databaseName
            )
        )
    }

    @Throws(MiniDBException::class, IOException::class, ClassNotFoundException::class)
    fun resume() {
        val file = File(absFilePath)
        if (!file.exists()) throw MiniDBException(String.format("Database %s disappears!", databaseName))
        val directories = file.listFiles { obj: File -> obj.isDirectory } ?: return
        for (each in directories) {
            val schema = Schema(each.name, databaseName)
            schema.resume()
            schemas[each.name] = schema
        }
    }

    @Throws(IOException::class, MiniDBException::class)
    fun close() {
        for (schema in schemas.values) {
            schema.close()
        }
    }

    @Throws(MiniDBException::class, IOException::class)
    fun addSchema(name: String, schema: Schema) {
        if (schemas.containsKey(name)) throw MiniDBException(
            String.format(
                "Database %s already contains a schema named %s!",
                databaseName,
                name
            )
        )
        val path = Paths.get(absFilePath, name).toString()
        if (!File(path).mkdir()) throw MiniDBException(
            String.format(
                "Failed to create the schema %s. Cannot create directory.",
                name
            )
        )
        try {
            schema.create()
        } catch (e: Exception) {
            Misc.rmDir(path)
            throw e
        }
        schemas[name] = schema
    }

    @Throws(MiniDBException::class, IOException::class)
    fun dropSchema(name: String) {
        if (!schemas.containsKey(name)) throw MiniDBException(
            String.format(
                "Database %s dose not contain a table named %s!",
                databaseName,
                name
            )
        )
        val schema: Schema? = schemas[name]
        schema!!.dropAllTable()
        schemas.remove(name)
    }

    val schemaNames: ArrayList<String>
        get() = ArrayList(schemas.keys)

    fun getSchemas(name: String): Schema? {
        return schemas[name]
    }
}
