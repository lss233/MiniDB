package com.lss233.minidb.engine.memory

import java.util.concurrent.ConcurrentHashMap

class Schema {
    val tables = ConcurrentHashMap<String, Table>()
}