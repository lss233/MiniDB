package com.lss233.minidb.engine.storage

import com.lss233.minidb.engine.config.MiniDBConfig
import com.lss233.minidb.exception.MiniDBException
import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Paths
import kotlin.io.path.Path

fun main() {
    val testRelation = TestRelation()
    testRelation.resumeDatabaseTest()
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
        meta.superKeys.add(ArrayList(mutableListOf(0)))
        meta.indices = ArrayList()
        meta.indices.add(ArrayList(mutableListOf(1)))
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


    fun resumeDatabaseTest() {
        val file = File(MiniDBConfig.DATA_FILE)
        if (!file.exists()) throw RuntimeException(String.format("MiniDB Storage File Path Wrong!"))
        val databases = file.listFiles { obj: File -> obj.isDirectory } ?: return
        for (each in databases) {
            println(each.absolutePath)
            resumeSchemaTest(each.name)
        }
    }

    private fun resumeSchemaTest(databaseName:String) {
        val schemaFile = File(Path(MiniDBConfig.DATA_FILE, databaseName).toString())
        if (!schemaFile.exists()) throw RuntimeException("Database $databaseName not exist!")
        val schemas = schemaFile.listFiles { obg: File -> obg.isDirectory } ?: return
        for (each in schemas) {
            println("\t ${each.name}")
            resumeTableTest(databaseName, each.name)
        }
    }

    private fun resumeTableTest(databaseName: String, schemaName: String) {
        val tableFile = File(Paths.get(MiniDBConfig.DATA_FILE, databaseName, schemaName).toString())
        if (!tableFile.exists()) throw RuntimeException("Wrong!!!")
        val tables = tableFile.listFiles { obj: File -> obj.isDirectory } ?: return
        for (table in tables) {
            println("\t\tTable ${table.name} was resume!")
        }
    }

    fun mkdirTest() {
        println(File("E:\\MiniDB-Data\\Data\\minidb\\a").mkdirs())
        val a : Boolean = true
        val c = if (a) 1 else 0
        println(c)
    }
}
