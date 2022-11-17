package com.lss233.minidb.engine.memory

import kotlin.collections.HashMap

class Database(val name: String, val dba: Int, val encoding: Int, val locProvider: Char, val allowConn: Boolean, val connLimit: Int) {
    var tables = HashMap<String, Table>();

    fun createTable(table: Table): Database {
        if(tables.containsKey(table.name)) {
            throw RuntimeException("Database ${table.name} already exists.")
        }
        tables[table.name] = table
        // TODO insert table info
        return this
    }
    operator fun set(tableName: String, table: Table) {
        tables[tableName] = table
    }


}