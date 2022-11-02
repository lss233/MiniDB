package com.lss233.minidb.engine

import com.lss233.minidb.utils.OrderPair

/**
 * @author  <a href="mailto:icebigpig404@foxmail.com">icebigpig</a>
 * @date    2022/10/5 11:20
 * @version 1.0
 */

class Attribute<V: OrderPair<*>>(private var colName: String) : ArrayList<V>() {


    fun serialize() {

    }

    fun deserialize() {

    }

    fun setColName(colName: String) {
        this.colName = colName
    }

    fun getColName():String? {
        return this.colName
    }
}