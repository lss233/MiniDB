package com.lss233.minidb.engine.memory

import com.lss233.minidb.engine.config.MiniDBConfig
import com.lss233.minidb.exception.MiniDBException
import com.lss233.minidb.utils.Misc
import java.io.File
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap

/**
 * Schema storage structure.
 * @param schemaName Indicate the name of the current schema.
 * @param belongDatabase Indicates which database it belongs to.
 */
class Schema(private val schemaName: String, private var belongDatabase: String): ConcurrentHashMap<String, View>() {

    private var tables = ConcurrentHashMap<String, View>()

    // The absolute location of the file on the system disk.
    private var absFilePath:String = Paths.get(MiniDBConfig.DATA_FILE, belongDatabase, schemaName).toString()

    init {
        createSchema()
    }

    fun createTable(table: Table):Schema {
        // TODO 这里需要创建关系表文件
        this[table.tableName] = table
        return this
    }

    private fun createSchema() {
        if (!File(absFilePath).exists()) {
            if (File(absFilePath).mkdirs()) {
                println(
                    String.format(
                        "Successfully created schema %s belonging to database %s.",
                        schemaName,
                        belongDatabase
                    )
                )
            } else {
                throw RuntimeException(
                    String.format(
                        "Failed to create the schema %s. Cannot create directory.",
                        this.schemaName
                    )
                )
            }
        }
    }

    override operator fun get(key: String) : View
            = tables[key] ?: throw RuntimeException("View $key does not exist.")

    operator fun set(name: String, view: View) {
        if (this.containsKey(name)) throw MiniDBException(
            String.format(
                "Schema %s already has a view named %s",
                schemaName,
                name
            )
        )
        tables[name] = view
    }

    /**
     * When the set method is executed, the data operated on is written to disk.
     */
    operator fun set(name: String, table: Table) {
        if (this.containsKey(name)) throw RuntimeException(
            String.format(
                "Schema %s already contains a table named %s!",
                schemaName,
                name
            )
        )
        val path = Paths.get(absFilePath, name).toString()
        if (!File(path).mkdirs()) throw RuntimeException(
            String.format(
                "Failed to create the table %s. Cannot create directory.",
                name
            )
        )
        table.absTableDirectory = path
        try {
            table.create()
        } catch (e: Exception) {
            throw e
        }
        tables[name] = table
    }

    override fun containsKey(key: String): Boolean {
        return tables.containsKey(key)
    }

    override fun remove(key: String): View? {
        if (!this.containsKey(key)) throw RuntimeException(
            String.format(
                "Schema %s dose not contain a table named %s!",
                schemaName,
                key
            )
        )
        val table: Table = this[key] as Table
        table.drop()
        return this.remove(key)
    }

    /**
     * Refresh the schema on disk.
     * Pre-read table information entries.
     */
    fun resumeTables() {
        val file = File(absFilePath)
        if (!file.exists()) throw RuntimeException(String.format("Schema %s disappears!", schemaName))
        val tables = file.listFiles { obj: File -> obj.isDirectory } ?: return
        for (each in tables) {
            val table = Table(each.name, belongDatabase, this.schemaName)
            table.resume()
            this[each.name] = table
        }
    }
}
