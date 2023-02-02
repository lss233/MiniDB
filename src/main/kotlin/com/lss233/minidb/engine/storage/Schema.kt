package com.lss233.minidb.engine.storage

import com.lss233.minidb.engine.config.MiniDBConfig
import com.lss233.minidb.engine.storage.View
import com.lss233.minidb.exception.MiniDBException
import com.lss233.minidb.utils.Misc
import java.io.File
import java.io.IOException
import java.nio.file.Paths

class Schema(schemaName: String, dbName: String) {

    private var schemaName: String

    // the relationTables in the schema
    private var relationTables: HashMap<String,RelationTable> = HashMap()

    private var absFilePath:String = Paths.get(MiniDBConfig.DATA_FILE, dbName, schemaName).toString()


    init {
        this.schemaName = schemaName
    }

    constructor(dbName: String) : this(dbName, "defaultSchema")

    @Throws(MiniDBException::class)
    fun create() {
        val file = File(absFilePath)
        if (file.exists()) throw MiniDBException(String.format("Schema %s already exists!", schemaName))
        if (!file.mkdir()) throw MiniDBException(
            String.format(
                "Failed to create the schema %s. Cannot create directory.",
                schemaName
            )
        )
    }

    @Throws(MiniDBException::class, IOException::class)
    fun dropRelationTable(name: String) {
        if (!relationTables.containsKey(name)) throw MiniDBException(
            String.format(
                "Schema %s dose not contain a table named %s!",
                schemaName,
                name
            )
        )
        val relation: RelationTable? = relationTables[name]
        relation!!.drop()
        relationTables.remove(name)
    }

    @Throws(MiniDBException::class, IOException::class)
    fun dropAllTable() {
        if (!relationTables.containsKey(absFilePath)) throw MiniDBException(
            String.format(
                "Schema %s dose not exist!",
                schemaName
            )
        ) else {
            Misc.rmDir(absFilePath)
        }
    }

    @Throws(IOException::class, MiniDBException::class)
    fun close() {
        for (relation in relationTables.values) {
            relation.close()
        }
    }

    @Throws(MiniDBException::class, IOException::class)
    fun addRelation(name: String, relation: RelationTable) {
        if (relationTables.containsKey(name)) throw MiniDBException(
            String.format(
                "Schema %s already contains a table named %s!",
                schemaName,
                name
            )
        )
        val path = Paths.get(absFilePath, name).toString()
        if (!File(path).mkdir()) throw MiniDBException(
            String.format(
                "Failed to create the table %s. Cannot create directory.",
                name
            )
        )
        relation.directory = path
        try {
            relation.create()
        } catch (e: Exception) {
            Misc.rmDir(path)
            throw e
        }
        relationTables[name] = relation
    }

    @Throws(MiniDBException::class, IOException::class)
    fun dropRelation(tableName: String) {
        if (!relationTables.containsKey(tableName)) throw MiniDBException(
            String.format(
                "Schema %s dose not contain a table named %s!",
                schemaName,
                tableName
            )
        )
        val relation: RelationTable? = relationTables[tableName]
        relation!!.drop()
        relationTables.remove(tableName)
    }

    @Throws(MiniDBException::class, IOException::class, ClassNotFoundException::class)
    fun resume() {
        val file = File(absFilePath)
        if (!file.exists()) throw MiniDBException(String.format("Schema %s disappears!", schemaName))
        val directories = file.listFiles { obj: File -> obj.isDirectory } ?: return
        for (each in directories) {
            val relation = RelationTable()
            relation.directory = each.absolutePath
            relation.resume()
            relationTables[each.name] = relation
        }
    }
}
