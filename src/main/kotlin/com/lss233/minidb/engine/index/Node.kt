package com.lss233.minidb.engine.index


class Node<E>(var nodeSize: Int, public var isLeaf: Boolean) {
    var keys: ArrayList<E> = ArrayList()
    var pointers: ArrayList<Any> = ArrayList()
    var next: Node<E>? = null
    var prev: Node<E>? = null

}
