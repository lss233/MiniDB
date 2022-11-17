package com.lss233.minidb.engine.storage.executor

import com.lss233.minidb.engine.config.DbStorageConfig
import com.lss233.minidb.engine.storage.struct.DbStruct
import com.lss233.minidb.engine.storage.struct.DbTableStruct
import com.lss233.minidb.engine.storage.type.StoredType.*
import com.lss233.minidb.utils.ByteUtil


class Insert {

    /**
     * 名称列表 需要插入的字段？
     */
    private var nameList = ArrayList<String>()

    /**
     * 数据列表
     */
    var dataList = ArrayList<Any>()

    /**
     * 对应的长度列表
     */
    private var lenList = ArrayList<Int>()

    /**
     * Insert的表结构
     */
    private var struct: DbTableStruct? = null


    fun doInsert(
        transactionId: Int,
        dbName: String,
        tableName: String,
        dataMap: HashMap<String, Any>
    ): ByteArray {
        // 这里假设表的结构是  {#uid，user_name,passwd}
//        final String dbName    = "test";
//        final String tableName = "db_user";
//        final HashMap<String,Object> dataMap = new HashMap<>();
//        dataMap.put("uid",1);
//        dataMap.put("user_name","testasdf");
//        dataMap.put("passwd","test");
        // TODO 记得清这里的注释

        // 读取文件数据到内存并获取到指定的表
        val struct = DbStruct.getTableStructByName(dbName, tableName)

        this.struct = struct
        nameList = struct!!.fieldNameList
        lenList = struct.fieldLensList

        // 开始赋值 遍历当前操作表中的所有字段，并插入指定的数据
        for (i in 0 until struct.fieldNum) {
            // 说明没传这个值 那么插入空
            dataList.add(i, dataMap.getOrDefault(nameList[i], null)!!)
            // TODO null 如何处理？
        }

        // dataList.add(1); dataList.add("testasdfa"); dataList.add("test");
        lenList.add(4)
        lenList.add(20)
        lenList.add(25)

        // 这里结束  下边开始处理业务
        // 总长度 = 表的数据长度 + 数据库存储表头的长度 + 字段长度
        val totalBytes: Int = struct.recordLen + DbStorageConfig.getTotalByteLen() + (struct.fieldNum * 4)

        // 交由构建存储->插入数据

        // 交由构建存储->插入数据
        return buildStorageBytes(transactionId, totalBytes, dataMap)
    }

    /**
     * 构建存储结构，暂时不考虑单行超出Page最大存储长度（16K）
     * @param transactionId 事务ID
     * @param totalBytes 构建的总byte长度
     * @param data 表数据
     * @return 构建存储的byte流
     */
    private fun buildStorageBytes(
        transactionId: Int,
        totalBytes: Int,
        data: HashMap<String, Any>
    ): ByteArray {
        // 存储构建的比特流
        val storageBytes = ByteArray(totalBytes)

        // 先拷贝信息头
        // 时间戳
        val timeStampByte4: ByteArray = ByteUtil.getTimeStampByte4()
        // TODO 这里改成从Config中读取
        System.arraycopy(timeStampByte4, 0, storageBytes, 4, 4) //拷贝时间戳
        // 记录事务ID
        val transactionIdByte: ByteArray = ByteUtil.intToByte4(transactionId)
        // TODO 这里改成从Config中读取
        System.arraycopy(transactionIdByte, 0, storageBytes, 8, 4) //拷贝事务id

        //接下来开始拷贝真正的信息
        // 这里是跳过了存储文件的头部信息
        var dataLenPos = DbStorageConfig.getTotalByteLen()
        // 这里是跳过字段信息（从这里后全是数据）
        // TODO 改成从Config中读取长度
        var realDataPos = dataLenPos + nameList.size * 4
        var dataTemp: Any
        // 对存储的数据进行字符转换处理
        for (i in nameList.indices) {
            dataTemp = dataList[i]
            when (struct?.fieldTypeList?.get(i)) {
                INT -> {
                    ByteUtil.arraycopy(storageBytes, realDataPos, ByteUtil.intToByte4(dataTemp as Int))
                    realDataPos += 4
                }

                CHAR -> {
                    val str = dataTemp.toString() //拷贝长度
                    ByteUtil.arraycopy(storageBytes, dataLenPos, ByteUtil.intToByte4(str.length))
                    ByteUtil.arraycopy(storageBytes, realDataPos, str.toByteArray()) //拷贝真实信息
                    realDataPos += lenList[i]
                }

                TIME_STAMP -> {
                    ByteUtil.arraycopy(storageBytes, realDataPos, ByteUtil.getTimeStampByte4())
                    realDataPos += 4
                }
                BIGINT -> TODO()
                VARCHAR -> TODO()
                DATA_TIME -> TODO()
                null -> TODO()
            }
            dataLenPos += 4
        }
        return storageBytes
    }
}
