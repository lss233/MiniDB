package com.lss233.minidb.utils

import java.io.File
import java.nio.charset.Charset

/**
 * 读取文件数据的工具类，工具类内的方法可以对当前数据库内的数据进行读写操作
 * @param dataBaseName 使用时需要先构造一个所操作数据库的名称
 */
class FileUtil (private val dataBaseName:String){

    // TODO 文件系统设计按照不同数据库拥有单独的文件夹，文件夹下存放所有的表
    private val currentDir = System.getProperty(".") + "\\out"

    private val suffix = ".mdb"

    /**
     * 以字节流方式读取整个表格文件
     * @param tableName 表格名（无需加后缀）
     */
    fun readFileAsByteArray(tableName:String): ByteArray {
        val file = File(tableName + suffix)
        return file.readBytes()
    }

    /**
     * 以字符流方式读取整个表格文件
     * @param tableName 表格名（无需加后缀）
     */
    fun readFileAsString(tableName:String): String {
        val file = File(tableName + suffix)
        return file.readBytes().toString(Charset.defaultCharset())
    }

    // TODO 对于文件的读取，可能需要进行考虑设计是否需要块读取（部分读取）

    /**
     * 覆盖原有文件进行写入比特流
     * @param tableName 需要重写的表名
     * @param data 需要写入的比特流数据
     */
    fun overrideWriteFile(tableName: String, data:ByteArray) {
        val file = File(tableName + suffix)
        file.writeBytes(data)
    }

    /**
     * 从尾部追加表格数据
     * @param tableName 表名
     * @param data 追加的数据
     */
    fun appendWriteFile(tableName: String, data:ByteArray) {
        val file = File(tableName + suffix)
        file.appendBytes(data)
    }
}