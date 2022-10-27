package com.lss233.minidb.utils

/**
 * @author  <a href="mailto:icebigpig404@foxmail.com">icebigpig</a>
 * @date    2022/10/14 11:09
 * @version 1.0
 */

class OrderPair<V : Any>(private var order: Int, value: V) {

    private var value: V? = value

    fun getOrder(): Int {
        return this.order
    }

    fun getValue(): V? {
        return this.value
    }
}
