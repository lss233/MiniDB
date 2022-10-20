package com.lss233.minidb.engine

class NTuple: ArrayList<Any>() {
    operator fun plus(element: Any?): NTuple {
        val result = NTuple()
        result.addAll(this)
        result.add(element!!)
        return result
    }

    companion object {
        fun from(vararg items: Any): NTuple {
            val tuple = NTuple();
            tuple.addAll(items)
            return tuple
        }
    }
}