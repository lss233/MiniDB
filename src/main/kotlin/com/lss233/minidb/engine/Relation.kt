package com.lss233.minidb.engine

import com.lss233.minidb.engine.schema.Column
import com.lss233.minidb.utils.ConsoleTableBuilder
import miniDB.parser.ast.expression.Expression
import miniDB.parser.ast.expression.primary.Identifier
import java.util.*
import java.util.function.BiPredicate
import java.util.function.Predicate
import kotlin.collections.ArrayList
import kotlin.collections.HashSet

/**
 * 关系
 * Each relation is made of either a list of Tuples
 * Or many list of Domains
 */
open class Relation(val columns: MutableList<Column>, val rows: MutableList<Array<Any>>) {
    var alias: String = UUID.randomUUID().toString()
        set(value) {
            for(col in columns) {
                if(col.identifier.parent == null) {
                    col.identifier.parent = Identifier(col.identifier.parent?.parent, value)
                }
            }
            field = value
        }
    val tuples: MutableList<NTuple> = rows.map { cols -> run {
            val tuple = NTuple()
            cols.forEachIndexed { index, any ->
                tuple.add(Cell(columns[index], any))
            }
            tuple
        }
    }.toMutableList();

    fun clone() = Relation(mutableListOf<Column>().also { it.addAll(columns) }, mutableListOf<Array<Any>>().also { it.addAll(rows) })



    private fun projection(projectColumn: MutableList<Column>): Relation {
        val projectColumnIndexes = projectColumn.map { this.columns.indexOf(it) }.toMutableList()
        val projectRow = this.rows.map { oldRow -> run {
            Array(projectColumnIndexes.size)
            { index -> oldRow[projectColumnIndexes[index]] }
        } }.toMutableList()
        return Relation(projectColumn, projectRow)
    }

    /**
     * 选择算子
     * 保留列信息，筛选行
     */
    infix fun select(cond: Predicate<NTuple>): Relation {
        return Relation(columns, tuples.filter { i -> cond.test(i) }.map{ it.toArray() }.toMutableList())
    }

    /**
     * 投影算子
     */
    infix fun projection(cond: Predicate<Column>): Relation =
         projection(columns.filter { i -> cond.test(i)}.toMutableList());

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
        return Relation(columns.toMutableList(), rows.map { it.toArray() }.toMutableList());
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
        return Relation(columns.toMutableList(), rows.map { it.toArray() }.toMutableList());
    }
    fun naturalJoin(relation: Relation) {

    }


    fun union(relation: Relation): Relation {
        if(relation.columns.size != columns.size)
            throw RuntimeException("每一个 UNION 查询必须有相同的字段个数")
        val columns = this.columns
        val rows = this.rows + relation.rows
        return Relation(columns.toMutableList(), rows.toMutableList())
    }

    override fun toString(): String =
        ConsoleTableBuilder()
            .withHeaders(*columns.map{ i -> i.getFullName() }.toTypedArray())
            .withBody(*tuples.toTypedArray())
            .build()


    companion object {
        fun empty(): Relation {
            return Relation(mutableListOf(), mutableListOf())
        }
    }
}
