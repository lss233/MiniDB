package com.lss233.minidb.engine.index

import miniDB.index.BPTree
import java.io.BufferedReader
import java.io.FileReader
import java.io.IOException
import java.util.*

class Dictionary {
    // testing purpose
    private val dictionay: BPTree<String> = BPTree<String>(200, 200, 3)

    @Throws(IOException::class)
    fun loadDic(path: String?) {
        val fr = FileReader(path)
        val r = BufferedReader(fr)
        var s = r.readLine()
        val dic = ArrayList<String>()
        val record = ArrayList<Any>()
        var count = 0
        while (s != null) {
            dic.add(s.lowercase(Locale.getDefault()))
            record.add(s.lowercase(Locale.getDefault()))
            s = r.readLine()
            count++
        }
        println(count)
        dictionay.insertBulk(dic, record)
    }

    fun search(s: String): Boolean {
        val n: Node<String>? = dictionay.search(s.lowercase(Locale.getDefault()))
        return n != null
    }

}