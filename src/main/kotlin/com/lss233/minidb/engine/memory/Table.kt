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
    fun insert(row: Array<Any>) {
        if(row.size != columns.size) {
            throw RuntimeException("Unable inserting row for table `$name`, incorrect row size and column size.")
        }
        this.rows.add(row)
    }
    fun insert(row: NTuple) {
        val tuple = NTuple()
        val arr = ArrayList<Any>()

        for(column in columns) {
            if(row.columns.contains(column)) {
                tuple.add(row[column])
                arr.add((row[column] as Cell<*>).value!!)
            } else {
                tuple.add(Cell(column, column.defaultValue() ?: throw RuntimeException("Unable inserting row for table `$name`, no default value for column `${column.name}`.")))
                arr.add(column.defaultValue()!!)
            }
        }
        tuples.add(tuple)
        insert(arr.toArray())
    }

}