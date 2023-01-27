package com.lss233.minidb.engine.storage

import com.lss233.minidb.exception.MiniDBException
import com.lss233.minidb.utils.Misc
import java.io.File
import java.io.IOException
import java.nio.file.Paths


class Database {
    // the directory to store the database
    var directory: String
    // the relations in the database
    var relations: ArrayList<String>

    var tableNameToTable: HashMap<String, RelationTable>

    constructor() {
        directory = ""
        relations = ArrayList()
        tableNameToTable = HashMap()
    }

    constructor(directory: String) {
        this.directory = directory
        relations = ArrayList()
        tableNameToTable = HashMap()
    }

    @Throws(MiniDBException::class)
    fun create() {
        val file = File(directory)
        if (file.exists()) throw MiniDBException(String.format("Database %s already exists!", directory))
        if (!file.mkdir()) throw MiniDBException(
            String.format(
                "Failed to create the database %s. Cannot create directory.",
                directory
            )
        )
    }

    @Throws(MiniDBException::class, IOException::class, ClassNotFoundException::class)
    fun resume() {
        val file = File(directory)
        if (!file.exists()) throw MiniDBException(String.format("Database %s disappears!", directory))
        val directories = file.listFiles { obj: File -> obj.isDirectory } ?: return
        for (each in directories) {
            println(each)
            val relation = RelationTable()
            relation.directory = each.absolutePath
            relation.resume()
            tableNameToTable[each.name] = relation
        }
    }

    @Throws(IOException::class, MiniDBException::class)
    fun close() {
        for (relation in tableNameToTable.values) {
            relation.close()
        }
    }

    @Throws(MiniDBException::class, IOException::class)
    fun addRelation(name: String, relation: RelationTable) {
        if (tableNameToTable.containsKey(name)) throw MiniDBException(
            String.format(
                "Database %s already contains a table named %s!",
                directory,
                name
            )
        )
        val path = Paths.get(directory, name).toString()
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
        tableNameToTable[name] = relation
    }

    @Throws(MiniDBException::class, IOException::class)
    fun dropRelation(name: String) {
        if (!tableNameToTable.containsKey(name)) throw MiniDBException(
            String.format(
                "Database %s dose not contain a table named %s!",
                directory,
                name
            )
        )
        val relation: RelationTable? = tableNameToTable[name]
        relation!!.drop()
        tableNameToTable.remove(name)
    }

    val relationNames: ArrayList<String>
        get() = ArrayList(tableNameToTable.keys)

    fun getRelation(name: String): RelationTable? {
        return tableNameToTable[name]
    }
}
