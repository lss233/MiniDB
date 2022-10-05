package com.lss233.minidb.engine

import java.util.function.Consumer
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
        return tuples.filter { i -> predicate.test(i) }.toList();
    }

}