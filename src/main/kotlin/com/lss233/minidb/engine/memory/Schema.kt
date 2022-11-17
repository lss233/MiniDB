package com.lss233.minidb.engine.memory

import java.util.concurrent.ConcurrentHashMap

class Schema(schemaName: String) {
    private val tables = ConcurrentHashMap<String, Table>()
    operator fun set(name: String, table: Table) {
        tables[name] = table
    }
    operator fun get(name: String) : Table {
        return tables[name] ?: throw RuntimeException("Table $name does not exist.")
    }
}