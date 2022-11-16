package com.lss233.minidb.engine.memory

import miniDB.parser.ast.expression.primary.Identifier
import java.util.*
import kotlin.collections.HashMap

object Engine {
    val databases = HashMap<String, Database>()
    operator fun get(identifier: Identifier) : Table? {
        var identifier_ = identifier
        val stack = Stack<Identifier>();
        stack.push(identifier_)
        while(identifier_.parent != null) {
            identifier_ = identifier_.parent
            stack.push(identifier_)
        }
        val db =  if(databases.containsKey(stack.peek().idText)) {
            databases[stack.pop().idText]
        } else {
            databases["pg_catalog"]
        }
        return db?.tables?.get(stack.pop().idText)
    }
}