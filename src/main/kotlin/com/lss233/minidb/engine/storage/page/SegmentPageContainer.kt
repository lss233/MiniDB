package com.lss233.minidb.engine.storage.page

import com.lss233.minidb.engine.config.DBConfig
import java.io.File

/**
 * 查找的时候在这里拿到跟id相关的page
 * 处理后再根据page的offset写回文件
 */
class SegmentPageContainer {

    /**
     * key：数据库名 value：table->segment->page
     */
    private var dbMap = HashMap<String, Map<String, List<Segment>>>()

    /**
     * key: 表名 value：页名
     */
    var tableMap = HashMap<String, List<Segment>>()

    /**
     * 向数据库中添加一个表
     */
    fun add(dbName: String, tableMap:HashMap<String, List<Segment>>) {
        dbMap[dbName] = tableMap
    }

    /**
     * 通过表名获取所有的Page
     */
    fun getAllPageByTableName(dbName: String?, tableName: String?): List<Page> {
        val pages = mutableListOf<Page>()
        for (segment in dbMap[dbName]!![tableName!!]!!) {
            segment.pages?.let { pages.addAll(it.toList()) }
        }
        return pages
    }

    /**
     * 通过页表ID获取页数据
     */
    // TODO 这里后期要进行查询优化
    fun getPageById(dbName: String?, table: String?, keyId: Int): Page {
        var page = Page()
        val segments = dbMap[dbName]!![table!!]
        if (segments != null) {
            for (segment in segments) {
                if (segment.maxId > keyId) { //说明id所在的page在这个段表里
                    for (pageTemp in segment.pages!!) {
                        if (pageTemp.minId!! < keyId && pageTemp.maxId!! > keyId) {
                            page = pageTemp
                        }
                    }
                }
            }
        }
        return page
    }

    /**
     * 获取表中最大的Page
     */
    fun getMaxPage(dbName: String?, tableName: String?): Page {
        val segmentList = dbMap[dbName]!![tableName!!]
        val segment = segmentList!![segmentList.size - 1]
        val pages: ArrayList<Page>? = segment.pages
        return pages!![pages.size - 1]
    }

    /**
     * 创建表并开辟一块空间
     */
    fun createBlankPage(dbName: String, tableName: String): Boolean {
        val page = Page()
        val data = ByteArray(16 * 1024)
        page.data = data
        val realFile: String = DBConfig.DB_ROOT_PATH + dbName + "_" + tableName + DBConfig.FILE_SUFFIX
        val file = File(realFile)
        var returnFlag = false
        try {
            if (!file.exists()) {
//                FileUtil.writeBytes(data, realFile)
                // TODO writeBytes To File
            }
            val maxPos = file.totalSpace.toInt()
//            FileUtil.writeBytes(data, file, maxPos, 16 * 1024, true)
            // TODO writeBytes To File With Pos
            returnFlag = true
        } catch (e: Exception) {
            println("create blank page error!")
        }
        return returnFlag
    }
}