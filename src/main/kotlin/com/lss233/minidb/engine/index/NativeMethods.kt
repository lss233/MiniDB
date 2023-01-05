package com.lss233.minidb.engine.index

import kotlin.math.ceil


class NativeMethods<E : Comparable<E>?> {
    fun sortedInsert(key: E, record: Any, N: Node<E>) {
        val data: ArrayList<E> = N.keys
        val pointers: ArrayList<Any> = N.pointers
        if (data.isEmpty()) {
            data.add(key)
            pointers.add(record)
            return
        } else {
            if (key!!.compareTo(data[0]) < 0) {
                data.add(0, key)
                pointers.add(0, record)
            } else {
                var flag = true
                for (i in data.indices) {
                    if (key.compareTo(data[i]) < 0) {
                        flag = false
                        data.add(i, key)
                        pointers.add(i, record)
                        break
                    }
                }
                if (flag) {
                    data.add(key)
                    pointers.add(record)
                }
            }
        }
    }

    fun sortedInsertInternal(key: E, record: Any, N: Node<E>) {
        val data: ArrayList<E> = N.keys
        val pointers: ArrayList<Any> = N.pointers
        if (key!!.compareTo(data[0]) < 0) {
            data.add(0, key)
            pointers.add(1, record)
        } else {
            var flag = true
            for (i in data.indices) {
                if (key.compareTo(data[i]) < 0) {
                    data.add(i, key)
                    pointers.add(i + 1, record)
                    flag = false
                    break
                }
            }
            if (flag) {
                data.add(key)
                pointers.add(record)
            }
        }
    }

    fun deleteNode(n: Node<E>, k: E) {
        for (i in 0 until n.keys.size) {
            if (k!!.compareTo(n.keys[i]) == 0) {
                n.keys.removeAt(i)
                n.pointers.remove(i)
            }
        }
    }

    fun internalDelete(key: E, n: Node<E>, temp: Node<E>?) {
        for (i in 0 until n.keys.size) {
            if (n.keys.get(i)!!.compareTo(key) === 0) {
                n.keys.removeAt(i)
                n.pointers.remove(i + 1)
            }
        }
    }

    fun sameParent(n: Node<E>, parent: Node<E>, size: Int): Int {
        val keys: ArrayList<E> = parent.keys
        var _next = false
        var _prev = false
        val next: Node<E> = n.next!!
        val prev: Node<E> = n.prev!!
        if (sameParent2(parent, n)) {
            for (i in 0 until parent.pointers.size) {
                if (next === parent.pointers[i]) {
                    _next = true
                    break
                }
            }
        }
        if (!sameParent2(parent, n)) {
            for (i in 0 until parent.pointers.size) {
                if (prev === parent.pointers[i]) {
                    _prev = true
                    break
                }
            }
        }
        return if (_next && next.keys.size - 1 >= ceil(size / 2.0)) {
            1
        } else if (_prev && prev.keys.size - 1 >= ceil(size / 2.0)) {
            2
        } else {
            0
        }
    }

    fun nexOrprev(n: Node<E>, parent: Node<E>, size: Int): Int {
        var _next = false
        var _prev = false
        val next: Node<E> = n.next!!
        val prev: Node<E> = n.prev!!
        if (next != null) {
            for (i in 0 until parent.pointers.size) {
                if (next === parent.pointers[i]) {
                    _next = true
                    break
                }
            }
        }
        if (prev != null) {
            for (i in 0 until parent.pointers.size) {
                if (prev === parent.pointers[i]) {
                    _prev = true
                    break
                }
            }
        }
        return if (next != null && _next && next.keys.size - 1 >= 1) {
            1
        } else if (prev != null && _prev && prev.keys.size - 1 >= 1) {
            2
        } else {
            0
        }
    }

    fun sameParent2(parent: Node<E>, n: Node<E>): Boolean {
        var _next = false
        var _prev = false
        val next: Node<E> = n.next!!
        val prev: Node<E> = n.prev!!
        if (next != null) {
            for (i in 0 until parent.pointers.size) {
                if (next === parent.pointers[i]) {
                    _next = true
                    break
                }
            }
        }
        if (prev != null) {
            for (i in 0 until parent.pointers.size) {
                if (prev === parent.pointers[i]) {
                    _prev = true
                    break
                }
            }
        }
        return _next
    }
}
