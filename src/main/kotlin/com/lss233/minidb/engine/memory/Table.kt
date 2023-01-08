package com.lss233.minidb.engine.memory

import com.lss233.minidb.engine.Cell
import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.Relation
import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.expression.Expression
import java.util.function.Predicate
class Table(val name: String, private val relation: Relation): View() {

    constructor(name: String, columns: MutableList<Column>, tuples: MutableList<NTuple>) :
            this(name, Relation(columns, tuples.map { it.toArray() }.toMutableList()))

    /**
     * Get a copy of relation
     * @return Relation of this table
     */
    override fun getRelation(): Relation
        = relation.clone()

    override fun insert(row: Array<Any>) {
        if(row.size != relation.columns.size) {
            throw RuntimeException("Unable inserting row for table `$name`, incorrect row size and column size.")
        }
        relation.rows.add(row)
    }
    override fun insert(row: NTuple) {
        val tuple = NTuple()
        val arr = ArrayList<Any>()

        for(column in relation.columns) {
            if(row.columns.contains(column)) {
                tuple.add(row[column])
                arr.add((row[column] as Cell<*>).value!!)
            } else {
                tuple.add(Cell(column, column.defaultValue() ?:
                    throw RuntimeException("Unable inserting row for table `$name`, " +
                            "no default value for column `${column.name}`.")))
                arr.add(column.defaultValue()!!)
            }
        }
        relation.tuples.add(tuple)
        insert(arr.toArray())
    }

    override fun update(cond: Predicate<NTuple>, updated: Array<Cell<Expression>>): Int {
        var affectsCounter = 0
        for (tuple in relation.tuples) {
            if(cond.test(tuple)) {
                affectsCounter ++
                for (cell in updated) {
                    tuple[cell.column] = cell.value
                }
            }
        }
        return affectsCounter
    }
}