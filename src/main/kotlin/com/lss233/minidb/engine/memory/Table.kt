package com.lss233.minidb.engine.memory

import com.lss233.minidb.engine.Cell
import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.Relation
import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.expression.primary.Identifier

class Table(val name: String, columns: MutableList<Column>, tuples: MutableList<NTuple>) : Relation(columns, tuples.map { it.toArray() }.toMutableList()) {

    fun getRelation(alias: String): Relation {
        val ret = clone()
        ret.alias = alias
        return ret
    }
    fun insert(row: NTuple) {
        val tuple = NTuple()
        for(column in columns) {
            if(row.columns.contains(column)) {
                tuple.add(row[column])
            } else {
                tuple.add(Cell(column, column.defaultValue() ?: throw RuntimeException("Unable inserting row for table `$name`, no default value for column `${column.name}`.")))
            }
        }
        tuples.add(tuple)
    }

}