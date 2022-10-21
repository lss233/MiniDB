package com.lss233.minidb.engine

class NTupleAbandon: ArrayList<Any>() {
    operator fun plus(element: Any?): NTupleAbandon {
        val result = NTupleAbandon()
        result.addAll(this)
        result.add(element!!)
        return result
    }

    companion object {
        fun from(vararg items: Any): NTupleAbandon {
            val tuple = NTupleAbandon();
            tuple.addAll(items)
            return tuple
        }
    }

}