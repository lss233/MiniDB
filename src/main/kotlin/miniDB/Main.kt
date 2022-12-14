package miniDB

import com.lss233.minidb.engine.Cell
import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.memory.Engine
import com.lss233.minidb.engine.schema.Column
import com.lss233.minidb.engine.storage.StorageService
import com.lss233.minidb.engine.storage.executor.Insert
import com.lss233.minidb.engine.storage.struct.DbStruct
import com.lss233.minidb.engine.storage.struct.DbTableField
import com.lss233.minidb.engine.storage.struct.DbTableStruct
import miniDB.parser.ast.fragment.ddl.ColumnDefinition
import miniDB.parser.ast.fragment.ddl.datatype.DataType
import java.io.File

fun main() {

    val storageService = StorageService()

    val column1 = Column("1")
    column1.definition = ColumnDefinition(
        DataType(DataType.DataTypeName.CHAR,  true, true, true,null,null,null,null,null),
        true,null,true,null,null,null,null,null,true,true,null
    )

    val column2 = Column("2")
    column2.definition = ColumnDefinition(
        DataType(DataType.DataTypeName.CHAR,  true, true, true,null,null,null,null,null),
        true,null,true,null,null,null,null,null,true,true,null
    )

    val column3 = Column("3")
    column3.definition = ColumnDefinition(
        DataType(DataType.DataTypeName.CHAR,  true, true, true,null,null,null,null,null),
        true,null,true,null,null,null,null,null,true,true,null
    )

    val column4 = Column("4")
    column4.definition = ColumnDefinition(
        DataType(DataType.DataTypeName.CHAR,  true, true, true,null,null,null,null,null),
        true,null,true,null,null,null,null,null,true,true,null
    )

    val tuples = ArrayList<NTuple>()

    val tuple = NTuple.from(Cell(column1,"asdf"), Cell(column2,"123"), Cell(column3,"ertregh"), Cell(column4,"adsgdsfg"))

    tuples.add(tuple)
    tuples.add(tuple)
    tuples.add(tuple)
    tuples.add(tuple)
    tuples.add(tuple)
    tuples.add(tuple)

    storageService.initStorageService()
    val databases = storageService.getDatabaseList()

    Engine.loadStorageData()
}

fun test(args: Array<String>) {
    // ???????????????
    val hashMap = HashMap<String, DbTableStruct>()

    // ????????????????????????
    val dbTableStruct = DbTableStruct()

    // ??????????????????????????????
    val dbTableField = DbTableField("test_col_")
    dbTableField.byteLen = 10
    dbTableField.type = DataType.DataTypeName.CHAR
    dbTableField.isPrimaryKey = true
    val dbTableFields = ArrayList<DbTableField>()
    dbTableFields.add(dbTableField)

    // ???????????????
    dbTableStruct.tableName = "testTable"

    // Add Fields
    dbTableStruct.fields = dbTableFields

    val integers = ArrayList<Int>()
    integers.add(1)
    dbTableStruct.fieldLensList = integers

    // ???????????????????????????
    hashMap[dbTableStruct.tableName!!] = dbTableStruct

    // ????????????????????????????????????????????????????????????????????????????????????
    DbStruct.add("testDB", hashMap)

    val insert = Insert()

    // ???????????????
    val insetDataMap = HashMap<String, Any>()

    insetDataMap.put("test_col_","123")

    // ????????????
    println(insert.doInsert(1,"testDB","testTable", insetDataMap))

    // TODO ???????????????????????????????????????
}