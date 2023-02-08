package com.lss233.minidb.engine.storage

import com.lss233.minidb.exception.MiniDBException
import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder

fun main() {
    val testRelation = TestRelation()
    testRelation.mkdirTest()
}
class TestRelation {

    @Throws(IOException::class, ClassNotFoundException::class, MiniDBException::class)
    fun testCreateRelation() {
        val meta = RelationMeta()
        meta.ncols = 3
        meta.colnames = ArrayList(mutableListOf("a", "b", "c"))
        meta.coltypes = ArrayList(listOf(java.lang.Integer::class.java, java.lang.Double::class.java, java.lang.String::class.java))

        meta.colsizes = ArrayList(mutableListOf(4, 8, 5))
        meta.nullableColIds = ArrayList(mutableListOf(2))
        meta.superKeys = ArrayList()
        meta.superKeys!!.add(ArrayList(mutableListOf(0)))
        meta.indices = ArrayList()
        meta.indices!!.add(ArrayList(mutableListOf(1)))
        val relation = RelationTable()
        relation.directory = "data/db/MyTable"
        File(relation.directory!!).mkdirs()
        relation.meta = meta
        relation.create()
        for (i in 0..99) {
            relation.insert(ArrayList(listOf(i, i.toDouble(), "you")))
        }
//        for (i in 0..89) {
//            relation.delete(i.toLong())
//        }
        relation.close()
        relation.resume()
        relation.data?.let { println(it.elementCount) }

    }


    fun test1() {
        val buffer = ByteArray(256)
        val bbuffer = ByteBuffer.wrap(buffer)
        bbuffer.order(ByteOrder.BIG_ENDIAN)
        bbuffer.putLong(1)
        bbuffer.putLong(2)
        bbuffer.position(0)
        println(bbuffer.long)
        println(bbuffer.long)
        // ;
        println(bbuffer.position())
    }

    fun mkdirTest() {
        println(File("E:\\MiniDB-Data\\Data\\minidb\\a").mkdirs())
        val a : Boolean = true
        val c = if (a) 1 else 0
        println(c)
    }
}
