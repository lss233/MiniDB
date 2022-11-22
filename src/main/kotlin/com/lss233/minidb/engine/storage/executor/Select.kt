package com.lss233.minidb.engine.storage.executor

import com.lss233.minidb.engine.config.DbStorageConfig
import com.lss233.minidb.engine.storage.struct.DbStruct
import com.lss233.minidb.engine.storage.struct.DbTableStruct
import com.lss233.minidb.engine.storage.type.StoredType.*
import com.lss233.minidb.utils.ByteUtil
import java.util.*

/**
 * 查询数据接口
 * TODO 暂时讲整个文件数据读取到内存中，进行Select操作
 */
class Select {


    var results = LinkedList<HashMap<String, Any>>()

    /**
     * 执行doSelect解析后的行数据
     */
    var rowData = HashMap<String, Any>()

    fun doSelect(dbName: String, tableName: String, data: ByteArray): LinkedList<HashMap<String, Any>> {
        val struct = DbStruct.getTableStructByName(dbName, tableName)
        //接下来开始根据 字段名 ->字段类型 ->字段长度 解析data 生成最终的结果
        val headerLen = DbStorageConfig.getTotalByteLen() //信息头的长度
        val storageLen: Int = struct!!.fieldNum * 4 //记录已存储信息的长度
        val realDataLen: Int = struct.recordLen  //插入的的信息的长度
        // 按行存储，需要对读取的比特流进行获取行部分数据
        val rowTotalLen = headerLen + storageLen + realDataLen
        // 对比特流进行按照行分割
        for (loop in 0 until data.size / rowTotalLen) {
            doOneRow((loop + 1) * rowTotalLen, headerLen, storageLen, realDataLen, data, struct)
        }
        println(results)
        return results
    }

    /**
     * 分割处理每一行的比特流
     * @param beginPos 开始的指针
     * @param headerLen 文件存储头部分的长度
     * @param storageLen 存储的长度
     * @param realDataLen 真实数据的长度
     * @param data 比特流数据
     * @param struct 表的结构
     */
    private fun doOneRow(beginPos: Int, headerLen: Int, storageLen: Int, realDataLen: Int, data: ByteArray, struct: DbTableStruct) {
        // TODO header 处理文件头部信息
        val isDelete = byte2Int4(0, data)
        val insertTimeStamp = byte2Int4(4, data)
        val transactionId = byte2Int4(8, data)
        val readTimeStamp = byte2Int4(12, data)
        val updateTimeStamp = byte2Int4(16, data)

        // body 处理文件体信息（数据部分）
        // 定义行存储开始指针

        val basePos: Int = beginPos + DbStorageConfig.getTotalByteLen()

        var lenPos: Int = beginPos + DbStorageConfig.getTotalByteLen()
        // 数据开始指针
        var dataPos: Int = basePos + struct.fieldNum * 4

        for (i in 0 until struct.fieldNameList.size) { //遍历那三个同步的List 处理一行数据
            when (struct.fieldTypeList[i]) {
                INT, TIME_STAMP -> {
                    val intTemp = byte2Int4(dataPos, data)
                    rowData[struct.fieldNameList[i]] = intTemp
                    lenPos += 4
                    dataPos += 4
                }
                VARCHAR -> {}
                CHAR -> {
                    val strLen = byte2Int4(lenPos, data)
                    val strByteTemp = ByteArray(strLen)
                    ByteUtil.arraycopy(data, dataPos, strByteTemp)
                    val strTemp = String(strByteTemp)
                    rowData[struct.fieldNameList[i]] = strTemp
                    lenPos += 4
                    dataPos += struct.fieldLensList[i]
                }

                BIGINT -> TODO()
                DATA_TIME -> TODO()
            }
        }
    }

    private fun byte2Int4(pos: Int, data: ByteArray): Int {
        return data[pos + 3].toInt() and 0xFF or (
                data[pos + 2].toInt() and 0xFF shl 8) or (
                data[pos + 1].toInt() and 0xFF shl 16) or (
                data[pos].toInt() and 0xFF shl 24)
    }
}