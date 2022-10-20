package com.lss233.minidb.utils

/**
 * @author  <a href="mailto:icebigpig404@foxmail.com">icebigpig</a>
 * @date    2022/10/14 11:09
 * @version 1.0
 */

class OrderPair<K : Any,V : Any>(key: K, value: V) {

    private var key: K? = key
    private var value: V? = value

    fun getKey(): K? {
        return this.key
    }

    fun getValue(): V? {
        return this.value
    }

}
