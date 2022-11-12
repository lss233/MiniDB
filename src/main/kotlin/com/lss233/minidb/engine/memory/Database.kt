package com.lss233.minidb.engine.memory

import miniDB.parser.ast.expression.primary.Identifier
import java.util.*
import kotlin.collections.HashMap

class Database {
    var tables = HashMap<String, Table>();

}