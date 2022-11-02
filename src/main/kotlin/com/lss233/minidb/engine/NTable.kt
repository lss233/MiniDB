package com.lss233.minidb.engine

import com.lss233.minidb.utils.OrderPair

/**
 * @author  <a href="mailto:icebigpig404@foxmail.com">icebigpig</a>
 * @date    2022/10/21 11:10
 * @version 1.0
 */

class NTable constructor(private var tableName:String, private var attributes:List<Attribute<OrderPair<*>>>) {

    fun getTableName():String {
        return this.tableName
    }

    fun setTableName(tableName: String) {
        this.tableName = tableName
    }

    fun setAttributes(attributes: List<Attribute<OrderPair<*>>>) {
        this.attributes = attributes
    }

    fun getAttributes(): List<Attribute<OrderPair<*>>> {
        return this.attributes
    }

    fun getNTuplesByLineNumber(startLine:Int, endLine:Int) : List<NTuple>{
        val nTuples = mutableListOf<NTuple>()

        return nTuples
    }

    fun getNTuple(line:Int): NTuple.Companion {
        val nTuple = NTuple
        nTuple.from()

        return nTuple
    }
}