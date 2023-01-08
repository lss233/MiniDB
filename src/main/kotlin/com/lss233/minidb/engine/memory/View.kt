package com.lss233.minidb.engine.memory

import com.lss233.minidb.engine.Cell
import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.Relation
import miniDB.parser.ast.expression.Expression
import java.util.function.Predicate

abstract class View {
    abstract fun getRelation(): Relation
    fun getRelation(alias: String): Relation {
        val ret = getRelation()
        ret.alias = alias
        return ret
    }

    open fun insert(row: Array<Any>) {
        throw UnsupportedOperationException("Cannot perform insert on view")
    }
    open fun insert(row: NTuple) {
        throw UnsupportedOperationException("Cannot perform insert on view")
    }
    open fun update(cond: Predicate<NTuple>, updated: Array<Cell<Expression>>): Int {
        throw UnsupportedOperationException("Cannot perform update on view")
    }
}