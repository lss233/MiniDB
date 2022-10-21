package com.lss233.minidb.engine

import com.lss233.minidb.engine.schema.Column
import com.lss233.minidb.utils.ConsoleTableBuilder
import java.util.function.BiPredicate

/**
 * 关系
 * Each relation is made of either a list of Tuples
 * Or many list of Domains
 */
class Relation(val columns: Array<Column>, val tuples: Array<NTupleAbandon>) {
    private fun projection(projectColumn: Array<Column>): Relation {
        val projectTuples = HashSet<NTupleAbandon>();

        for(column in projectColumn) {
            projectTuples.add(this.tuples[this.columns.indexOf(column)]);
        }
        return Relation(projectColumn, tuples)
    }
    /**
     * 选择算子
     */
    infix fun select(biPredicate: BiPredicate<NTupleAbandon, Relation>): Relation {
        return Relation(columns, tuples.filter { i -> biPredicate.test(i, this) }.toSet().toTypedArray())
    }

    /**
     * 投影算子
     */
    infix fun projection(biPredicate: BiPredicate<Column, Relation>):Relation {
        return projection(columns.filter { i -> biPredicate.test(i, this)}.toTypedArray());
    }

    /**
     * 连接算子
     */
    infix fun join(relation: Relation): Relation =
        Relation(
            setOf(*relation.columns).plus(this.columns).toTypedArray(),
            setOf(*relation.tuples).plus(this.tuples).toTypedArray())
    fun conditionalJoin(relation: Relation, condition: BiPredicate<NTupleAbandon, NTupleAbandon>) {
        
    }
    override fun toString(): String =
        ConsoleTableBuilder()
            .withHeaders(*columns.map{ i -> i.name }.toTypedArray())
            .withBody(*tuples)
            .build()
}
