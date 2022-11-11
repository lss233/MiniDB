package com.lss233.minidb.engine

import com.lss233.minidb.engine.schema.Column
import com.lss233.minidb.utils.ConsoleTableBuilder
import miniDB.parser.ast.expression.primary.Identifier
import java.util.*
import java.util.function.BiPredicate
import java.util.function.Predicate
import kotlin.collections.ArrayList

/**
 * 关系
 * Each relation is made of either a list of Tuples
 * Or many list of Domains
 */
open class Relation(val columns: Array<Column>, val rows: Array<Array<Any>>) {
    var alias: String = UUID.randomUUID().toString()
    val tuples: Array<NTuple>;
    init {
        tuples = rows.map { cols -> run {
            val tuple = NTuple()
            cols.forEachIndexed { index, any ->
                tuple.add(Cell(columns[index], any))
            }
            tuple
        }
        }.toTypedArray()
    }



    private fun projection(projectColumn: Array<Column>): Relation {
        val projectColumnIndexes = projectColumn.map { this.columns.indexOf(it) }.toTypedArray()
        val projectRow = this.rows.map { oldRow -> run {
            Array(projectColumnIndexes.size)
            { index -> oldRow[projectColumnIndexes[index]] }
        } }.toTypedArray()
        return Relation(projectColumn, projectRow)
    }

    /**
     * 选择算子
     * 保留列信息，筛选行
     */
    infix fun select(cond: Predicate<NTuple>): Relation {
        return Relation(columns, tuples.filter { i -> cond.test(i) }.map{ it.toArray() }.toTypedArray())
    }

    /**
     * 投影算子
     */
    infix fun projection(cond: Predicate<Column>): Relation =
         projection(columns.filter { i -> cond.test(i)}.toTypedArray());


    fun conditionalJoin(relation: Relation, condition: Predicate<NTuple>) : Relation {
        val columns = columns.map { i -> Column(Identifier(Identifier(null, alias), i.name)) }.toMutableList()
        columns.addAll(relation.columns.map { i -> Column(Identifier(Identifier(null, relation.alias), i.name)) })
        val rows = ArrayList<NTuple>();
        for(leftRow in tuples) {
            for(rightRow in relation.tuples) {
                val tuple = NTuple()
                tuple.addAll(leftRow)
                tuple.addAll(rightRow)
                if(condition.test(tuple)) {
                    rows.add(tuple)
                }
            }
        }
        return Relation(columns.toTypedArray(), rows.map { it -> it.toArray() }.toTypedArray());
    }
    fun outerJoin(relation: Relation, leftJoin: Boolean, condition: Predicate<NTuple>): Relation {
        val columns = columns.map { i -> Column(Identifier(Identifier(null, alias), i.name)) }.toMutableList()
        columns.addAll(relation.columns.map { i -> Column(Identifier(Identifier(null, relation.alias), i.name)) })
        val rows = ArrayList<NTuple>()
        if(leftJoin) {
            for(leftRow in tuples) {
                for(rightRow in relation.tuples) {
                    val tuple = NTuple()
                    tuple.columns = arrayListOf(*columns.toTypedArray())
                    tuple.addAll(leftRow)
                    tuple.addAll(rightRow)
                    if(condition.test(tuple)) {
                        rows.add(tuple)
                    }
                }
            }
        } else {
            for(rightRow in relation.tuples) {
                for(leftRow in tuples) {
                    val tuple = NTuple()
                    tuple.columns = arrayListOf(*columns.toTypedArray())
                    tuple.addAll(leftRow)
                    tuple.addAll(rightRow)
                    if(condition.test(tuple)) {
                        rows.add(tuple)
                    }
                }
            }
        }
        return Relation(columns.toTypedArray(), rows.map { it.toArray() }.toTypedArray());
    }
    fun naturalJoin(relation: Relation) {

    }

    override fun toString(): String =
        ConsoleTableBuilder()
            .withHeaders(*columns.map{ i -> i.getFullName() }.toTypedArray())
            .withBody(*tuples)
            .build()
}
