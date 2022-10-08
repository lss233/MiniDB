package com.lss233.minidb.engine

import com.lss233.minidb.utils.ConsoleTableBuilder
import java.util.function.BiPredicate

/**
 * 关系
 */
class Relation(val tuples: Set<NTuple>) {

    /**
     * 选择算子
     */
    infix fun select(biPredicate: BiPredicate<NTuple, Relation>): Relation {
        return Relation(tuples.filter { i -> biPredicate.test(i, this) }.toSet())
    }

    /**
     * 投影算子
     */
    infix fun projection(biPredicate: BiPredicate<NTuple, Relation>):Relation {
        return Relation(tuples.filter { i -> biPredicate.test(i, this) }.toSet())
    }

    /**
     * 连接算子
     */
    infix fun join(biPredicate: BiPredicate<NTuple, Relation>):Relation {
        return Relation(tuples.filter { i -> biPredicate.test(i, this) }.toSet())
    }

    override fun toString(): String {
        return ConsoleTableBuilder().withBody(*tuples.toTypedArray()).build();
    }
}
