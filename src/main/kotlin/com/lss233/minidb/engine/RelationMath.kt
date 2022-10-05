package com.lss233.minidb.engine

import java.util.stream.Collectors

class RelationMath {
    companion object {
        fun cartesianProduct(vararg args: List<Any>):List<List<Any>> {
            var result: List<List<Any>> = ArrayList();
//            args.flatMap {  }
            for (list in args) {
                if(result.isEmpty()) {
                    result = arrayListOf(list);
                } else {
                    result = result.stream().flatMap { row -> list.stream().map { item -> run {
                        row.stream().map { col -> run {
                            val arr = NTuple()
                            if(col is List<*>) {
                                col.forEach{i -> arr.add(i as Any)}
                            } else {
                                arr.add(col)
                            }
                            arr.add(item)
                            arr
                        } }.collect(Collectors.toList())
                    } }}.collect(Collectors.toList())
                }
            }
            val ret = ArrayList<List<Any>>()
            for (list in result) {
                for (arr in list) {
                    ret.add(arr as List<Any>)
                }
            }
            return ret
        }
        fun union(r: Set<Any>, s: Set<Any>): Set<Any> {
            val result = HashSet<Any>()
            result.addAll(r)
            result.add(s)
            return result
        }
        fun except(r: Set<Any>, s: Set<Any>): Set<Any> {
            val result = HashSet<Any>(r);
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