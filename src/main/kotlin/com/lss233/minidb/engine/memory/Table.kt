package com.lss233.minidb.engine.memory

import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.Relation
import com.lss233.minidb.engine.schema.Column

class Table(columns: Array<Column>, tuples: Array<Array<Any>>) : Relation(columns, tuples) {
    constructor(columns: Array<Column>, tuples: Array<NTuple>): this(columns, tuples.map { it.toArray() }.toTypedArray()) {
    }

}