package com.lss233.minidb.engine

import com.lss233.minidb.utils.ConsoleTableBuilder
import java.util.function.BiPredicate
import java.util.function.Predicate

/**
 * 关系
 */
class Relation(val tuples: Set<NTuple>) {

    /**
     * 选择算子
     */
    infix fun select(predicate: BiPredicate<NTuple, Relation>): Relation {
        return Relation(tuples.filter { i -> predicate.test(i, this) }.toSet());
    }

    override fun toString(): String {
        return ConsoleTableBuilder().withBody(*tuples.toTypedArray()).build();
    }

}