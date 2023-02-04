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

    // The absolute location of the file on the system disk.
    private var absFilePath:String = Paths.get(MiniDBConfig.DATA_FILE, belongDatabase, schemaName).toString()

    operator fun set(name: String, view: View) {
        this[name] = view
    }

    operator fun set(name: String, table: Table) {
        if (this.containsKey(name)) throw MiniDBException(
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
        table.absTableDirectory = path
        try {
            table.create()
        } catch (e: Exception) {
            Misc.rmDir(path)
            throw e
        }
        this[name] = table
    }

    override operator fun get(key: String) : View {
        if (!this.containsKey(key)) {
            throw RuntimeException("View $key does not exist.")
        }
        return this[key]
    }

    override fun remove(key: String): View? {
        if (!this.containsKey(key)) throw MiniDBException(
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
    fun resume() {
        val file = File(absFilePath)
        if (!file.exists()) throw MiniDBException(String.format("Schema %s disappears!", schemaName))
        val directories = file.listFiles { obj: File -> obj.isDirectory } ?: return
        for (each in directories) {
            val table = Table(each.name, belongDatabase, this.schemaName)
            table.absTableDirectory = each.absolutePath
            table.resume()
            this[each.name] = table
        }
    }
}
