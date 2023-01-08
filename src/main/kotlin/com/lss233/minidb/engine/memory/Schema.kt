package com.lss233.minidb.engine.memory

import java.util.concurrent.ConcurrentHashMap

class Schema(schemaName: String) {
    val views = ConcurrentHashMap<String, View>()

    operator fun set(name: String, view: View) {
        views[name] = view
    }

    operator fun set(name: String, table: Table) {
        views[name] = table
    }
    operator fun get(name: String) : View
        = views[name] ?: throw RuntimeException("View $name does not exist.")

}