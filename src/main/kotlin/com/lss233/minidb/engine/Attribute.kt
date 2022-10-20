package com.lss233.minidb.engine

/**
 * @author  <a href="mailto:icebigpig404@foxmail.com">icebigpig</a>
 * @date    2022/10/5 11:20
 * @version 1.0
 */

class Attribute<T> (colName:String, value:T){

    private var colName: String ?= colName

    private var value: T ?= value


    fun serialize() {

    }

    fun deserialize() {

    }

    fun getValue(): T? {
        return this.value
    }

}