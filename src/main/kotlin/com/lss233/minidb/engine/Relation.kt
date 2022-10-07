package com.lss233.minidb.engine

import com.lss233.minidb.utils.ConsoleTableBuilder
import java.util.function.Predicate

/**
 * 关系
 */
class Relation {

    val tuples = ArrayList<NTuple>()

    /**
     * 选择算子
     */
    infix fun select(predicate: Predicate<NTuple>):List<NTuple> {
        return tuples.filter { i -> predicate.test(i) }.toList()
    }


    /**
     * 投影算子
     */
    fun projection(predicate: Predicate<NTuple>):List<NTuple> {
        return tuples.filter { it -> predicate.test(it) }.toList()
    }


    override fun toString(): String {
        return ConsoleTableBuilder().withBody(*tuples.toTypedArray()).build();
    }

}