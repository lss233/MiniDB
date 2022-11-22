package miniDB

import com.google.gson.Gson
import com.lss233.minidb.engine.Cell
import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.schema.Column
import com.lss233.minidb.engine.storage.StorageService
import com.lss233.minidb.engine.storage.executor.Insert
import com.lss233.minidb.engine.storage.struct.DbStruct
import com.lss233.minidb.engine.storage.struct.DbTableField
import com.lss233.minidb.engine.storage.struct.DbTableStruct
import com.lss233.minidb.engine.storage.struct.TableField
import com.lss233.minidb.utils.ByteUtil
import miniDB.parser.ast.fragment.ddl.ColumnDefinition
import miniDB.parser.ast.fragment.ddl.datatype.DataType


fun main() {

    val storageService = StorageService()

    val column = Column("1")
    column.definition = ColumnDefinition(
        DataType(DataType.DataTypeName.INT,  true, true, true,null,null,null,null,null),
        true,null,true,null,null,null,null,null,true,true,null
    )

    val nTuple = NTuple.from(Cell(column,1))

    val tuples = ArrayList<NTuple>()

    tuples.add(nTuple)
    tuples.add(nTuple)
    tuples.add(nTuple)
    tuples.add(nTuple)
    tuples.add(nTuple)
    tuples.add(nTuple)

    println(storageService.buildStorageBytes(tuples))

}

fun aaa() {
    //Gson解析
    val gson = Gson()
    val fromJson = gson.fromJson("{}", TableField::class.java)
    println("Gson fromJson ==> $fromJson")
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