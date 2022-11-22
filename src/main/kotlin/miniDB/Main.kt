package miniDB

import com.lss233.minidb.engine.storage.StorageService
import com.lss233.minidb.engine.storage.executor.Insert
import com.lss233.minidb.engine.storage.struct.DbStruct
import com.lss233.minidb.engine.storage.struct.DbTableField
import com.lss233.minidb.engine.storage.struct.DbTableStruct
import com.lss233.minidb.utils.ByteUtil
import miniDB.parser.ast.fragment.ddl.datatype.DataType


fun main(args: Array<String>) {
    val storageService = StorageService()

//    storageService.initStorageService()

    val data: ByteArray

    val a = 999999999

    data = ByteUtil.intToByte4(a)

    println(data)

    println(ByteUtil.byteToInt4(data))
}

fun test(args: Array<String>) {
    // 单个数据库
    val hashMap = HashMap<String, DbTableStruct>()

    // 数据库中的单个表
    val dbTableStruct = DbTableStruct()

    // 为每个表添加测试字段
    val dbTableField = DbTableField("test_col_")
    dbTableField.byteLen = 10
    dbTableField.type = DataType.DataTypeName.CHAR
    dbTableField.isPrimaryKey = true
    val dbTableFields = ArrayList<DbTableField>()
    dbTableFields.add(dbTableField)

    // 创建表结构
    dbTableStruct.tableName = "testTable"

    // Add Fields
    dbTableStruct.fields = dbTableFields

    val integers = ArrayList<Int>()
    integers.add(1)
    dbTableStruct.fieldLensList = integers

    // 为数据库添加表文件
    hashMap[dbTableStruct.tableName!!] = dbTableStruct

    // 将封装的数据库添加到静态数据存储中（等同于所有的数据库）
    DbStruct.add("testDB", hashMap)

    val insert = Insert()

    // 插入的数据
    val insetDataMap = HashMap<String, Any>()

    insetDataMap.put("test_col_","123")



    // 写入文件
    println(insert.doInsert(1,"testDB","testTable", insetDataMap))


    // TODO 完成比特流到实体信息的解析



}