package com.lss233.minidb.engine.memory

import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.Relation
import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.expression.primary.Identifier

class Table(columns: Array<Column>, tuples: Array<Array<Any>>) : Relation(columns, tuples) {
    constructor(columns: Array<Column>, tuples: Array<NTuple>): this(columns, tuples.map { it.toArray() }.toTypedArray()) {
    }
    fun getRelation(alias: String): Relation {
        val ret = Relation(columns.clone(), rows.clone())
        ret.alias = alias
        return ret
    }

}