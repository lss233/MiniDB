package com.lss233.minidb.engine

import com.lss233.minidb.engine.schema.Column

class RelationMath {
    companion object {
//        fun cartesianProduct(vararg sets: Pair<Column, Set<Any>>): Relation =
//            Relation(
//                sets.map{ i -> i.first }.toSet().toTypedArray(),
//                sets.map { i -> i.second }.fold(listOf(NTupleAbandon())) { acc, set ->
//                    acc.flatMap { tuple -> set.map { element -> tuple + element } }
//                }
//                .toSet()
//                    .toTypedArray()
//            )

        fun <T> union(r: Set<T>, s: Set<T>): Set<T> {
            val result = HashSet<T>()
            result.addAll(r)
            result.addAll(s)
            return result
        }
        fun <T> except(r: Set<T>, s: Set<T>): Set<T> {
            val result = HashSet<T>(r);
            for(item in s) {
                if(result.contains(item)) {
                    result.remove(item)
                }
            }
            return result
        }
        fun intersection(r: Set<Any>, s: Set<Any>): Set<Any> {
            val result = HashSet<Any>();
            val check = if (r.size > s.size) r else s
            val scan = if (r.size > s.size) s else r
            for(item in scan) {
                if(check.contains(item)) {
                    result.add(item)
                }
            }
            return result
        }
    }
}