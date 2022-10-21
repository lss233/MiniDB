package com.lss233.minidb.engine

import com.lss233.minidb.utils.OrderPair

/**
 * @author  <a href="mailto:icebigpig404@foxmail.com">icebigpig</a>
 * @date    2022/10/21 11:10
 * @version 1.0
 */

class NTuple {

    private var attributes:List<Attribute<OrderPair<*>>> ?= null

    fun setAttributes(attributes: List<Attribute<OrderPair<*>>>) {
        this.attributes = attributes
    }

    fun getAttributes(): List<Attribute<OrderPair<*>>>? {
        return this.attributes
    }
}