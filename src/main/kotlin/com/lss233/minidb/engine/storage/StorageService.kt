package com.lss233.minidb.engine.storage

import cn.hutool.core.io.FileUtil
import com.google.gson.Gson
import com.lss233.minidb.engine.Cell
import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.config.DBConfig
import com.lss233.minidb.engine.config.DbStorageConfig
import com.lss233.minidb.engine.memory.Table
import com.lss233.minidb.engine.schema.Column
import com.lss233.minidb.engine.storage.struct.TableHeader
import com.lss233.minidb.utils.ByteUtil
import miniDB.parser.ast.fragment.ddl.datatype.DataType.DataTypeName.*
import java.io.File

class StorageService {

    fun initStorageService() {
        if (!FileUtil.exist(DBConfig.SYSTEM_FILE)) {
            println("System File Not Found, Initializing...")
            // TODO Create pg_System_tables : User information Table & Table information Table
            FileUtil.mkdir(DBConfig.SYSTEM_FILE)
        }
        if (!FileUtil.exist(DBConfig.DATA_FILE)) {
            println("Data File Not Found, Initializing...")
            FileUtil.mkdir(DBConfig.DATA_FILE)
        }
    }

    fun getTable(dbName:String, schemaName: String, tableName:String): Table {
        // Judge File existed
        val filePath = DBConfig.DATA_FILE + dbName + "\\" + schemaName + "\\" + tableName + DBConfig.TABLE_SUFFIX
        if (!FileUtil.exist(filePath)) {
            throw RuntimeException("ERROR: $tableName table is not existed.")
        }
        // Do One For Pages And Transform EntityCLass
        return parserTableBytes(FileUtil.readBytes(filePath))
    }

    fun getDatabaseList():MutableList<String> {
        val databaseList: MutableList<String> = mutableListOf()

        val databaseTree: FileTreeWalk = File(DBConfig.DATA_FILE).walk()
        databaseTree.maxDepth(1)
            .filter { it.isDirectory }
            .filter { it.name!="Data" }
            .forEach { databaseList.add(it.name) }
        return databaseList
    }

    fun getSchemaList(databaseName: String):MutableList<String> {
        val schemaList: MutableList<String> = mutableListOf()
        val schemaTree: FileTreeWalk = File(DBConfig.DATA_FILE + databaseName).walk()
        schemaTree
            .filter { it.isDirectory }
            .filter { it.name!= databaseName }
            .forEach { schemaList.add(it.name) }
        return schemaList
    }

    fun getTableList(databaseName: String, schemaName: String):MutableList<String> {
        val tableList:MutableList<String> = mutableListOf()
        val tableTree:FileTreeWalk = File(DBConfig.DATA_FILE + databaseName + "\\" + schemaName + "\\").walk()
        tableTree
            .filter { it.isFile }
            .filter { it.extension == "tb" }
            .forEach { tableList.add(it.name) }
        return tableList
    }

    fun createTable(table:Table, dbName: String, tableName: String) {
        val filePath = DBConfig.DATA_FILE + dbName + "\\"+ tableName + DBConfig.TABLE_SUFFIX
        if (FileUtil.exist(filePath)) {
            throw RuntimeException("ERROR: The $tableName table has existed.")
        }
        FileUtil.writeBytes(buildStorageBytes(table), filePath)
    }

    /**
     * 更新表或者保存表(is temp)
     * override file
     */
    fun updateOrSaveTable(table: Table, dbName: String, schemaName: String, tableName: String) {
        val filePath = DBConfig.DATA_FILE + dbName + "\\" + schemaName + "\\" + tableName + DBConfig.TABLE_SUFFIX
        FileUtil.writeBytes(buildStorageBytes(table), filePath)
    }

    private fun buildStorageBytes(table: Table):ByteArray {
        val tableHeader = TableHeader(tableName = table.name)
        val relation = table.getRelation()

        tableHeader.recordNumber = relation.tuples.size

        // Basic the columns form the table header info.
        tableHeader.columns = relation.columns

        // After parsing all columns, we can calculate the length occupied by a column
        val tupleSize = tableHeader.getColumnStorageSize()

        // totalBytesSize = Table header size + NTuple_Count * (The sum of every type that exist)
        val totalBytesSize = DbStorageConfig.TABLE_HEADER_SIZE + tableHeader.recordNumber * tupleSize
        
        val storageBytes = ByteArray(totalBytesSize)
        
        // Encoding tableHeader information: convert Object to Json
        val gson = Gson()
        val toJson = gson.toJson(tableHeader)
        val tableHeaderBytes = toJson.toByteArray(charset("UTF-8"))
        
        // mark header size
        ByteUtil.arraycopy(storageBytes, 0, ByteUtil.intToByte4(tableHeaderBytes.size))

        // Copy header info
        ByteUtil.arraycopy(storageBytes,4, tableHeaderBytes)

        var realDataPos = DbStorageConfig.TABLE_HEADER_SIZE

        // copy body info
        for (tuple in relation.tuples) {
            // order by table header
            for (column in tableHeader.columns) {
                println("bbb" + column.name)

                val dataTemp = tuple[column]
                when(column.definition.dataType.typeName!!) {
                    INT -> {
                        dataTemp as Cell<*>
                        ByteUtil.arraycopy(storageBytes, realDataPos, ByteUtil.intToByte4(dataTemp.value as Int))
                        realDataPos += 4
                    }
                    GEOMETRY -> TODO()
                        POINT -> TODO()
                        LINESTRING -> TODO()
                        POLYGON -> TODO()
                        MULTIPOINT -> TODO()
                        MULTILINESTRING -> TODO()
                        GEOMETRYCOLLECTION -> TODO()
                        MULTIPOLYGON -> TODO()
                        BIT -> TODO()
                        TINYINT -> TODO()
                        SMALLINT -> TODO()
                        MEDIUMINT -> TODO()
                        BIGINT -> TODO()
                        REAL -> TODO()
                        DOUBLE -> TODO()
                        FLOAT -> TODO()
                        DECIMAL -> TODO()
                        DATE -> TODO()
                        TIME -> TODO()
                        TIMESTAMP -> TODO()
                        DATETIME -> TODO()
                        YEAR -> TODO()
                        CHAR -> {
                            dataTemp as Cell<*>
                            val str = dataTemp.value.toString().toByteArray(charset("UTF-8"))
                        val strByteSize = str.size
                        // mark the length of chars
                        ByteUtil.arraycopy(storageBytes, realDataPos, ByteUtil.intToByte4(strByteSize))
                        realDataPos += 4
                        ByteUtil.arraycopy(storageBytes, realDataPos, str)
                        realDataPos += strByteSize
                    }
                    VARCHAR -> TODO()
                    BINARY -> TODO()
                    VARBINARY -> TODO()
                    TINYBLOB -> TODO()
                    BLOB -> TODO()
                    MEDIUMBLOB -> TODO()
                    LONGBLOB -> TODO()
                    TINYTEXT -> TODO()
                    TEXT -> TODO()
                    MEDIUMTEXT -> TODO()
                    LONGTEXT -> TODO()
                    ENUM -> TODO()
                    SET -> TODO()
                    BOOL -> TODO()
                    BOOLEAN -> TODO()
                    SERIAL -> TODO()
                    FIXED -> TODO()
                    JSON -> TODO()
                }
            }
        }
        return storageBytes
    }

    private fun parserTableHeaderBytes(headerByteArray: ByteArray): TableHeader {
        val headerSize = ByteUtil.byteToInt4(headerByteArray.copyOfRange(0,4))
        // slip the size of table header
        val gson = Gson()
        return gson.fromJson(String(headerByteArray.copyOfRange(4, headerSize + 4), charset("UTF-8")),TableHeader::class.java)
    }

    private fun parserTableBytes(bytes: ByteArray): Table {

        val nTuples = arrayListOf<NTuple>()

        val tableHeader = this.parserTableHeaderBytes(bytes.copyOfRange(0, DbStorageConfig.TABLE_HEADER_SIZE))
        // for every tuple
        for(index in 0 until tableHeader.recordNumber) {
            var realPos = DbStorageConfig.TABLE_HEADER_SIZE
            val nTuple = NTuple()
            for (column in tableHeader.columns) {
                when(column.definition.dataType.typeName!!) {
                    INT -> {
                        nTuple.add(Cell(Column(column.name), ByteUtil.byteToInt4(bytes.copyOfRange(realPos, realPos + 4))))
                        realPos += 4
                    }
                    GEOMETRY -> TODO()
                    POINT -> TODO()
                    LINESTRING -> TODO()
                    POLYGON -> TODO()
                    MULTIPOINT -> TODO()
                    MULTILINESTRING -> TODO()
                    GEOMETRYCOLLECTION -> TODO()
                    MULTIPOLYGON -> TODO()
                    BIT -> TODO()
                    TINYINT -> TODO()
                    SMALLINT -> TODO()
                    MEDIUMINT -> TODO()
                    BIGINT -> TODO()
                    REAL -> TODO()
                    DOUBLE -> TODO()
                    FLOAT -> TODO()
                    DECIMAL -> TODO()
                    DATE -> TODO()
                    TIME -> TODO()
                    TIMESTAMP -> TODO()
                    DATETIME -> TODO()
                    YEAR -> TODO()
                    CHAR -> {
                        // get the charsLength
                        val charLength = ByteUtil.byteToInt4(bytes.copyOfRange(realPos, realPos + 4))
                        realPos += 4
                        nTuple.add(Cell(Column(column.name), String(bytes.copyOfRange(realPos, realPos + charLength),
                            charset("UTF-8")
                        )))
                        realPos += charLength
                    }
                    VARCHAR -> TODO()
                    BINARY -> TODO()
                    VARBINARY -> TODO()
                    TINYBLOB -> TODO()
                    BLOB -> TODO()
                    MEDIUMBLOB -> TODO()
                    LONGBLOB -> TODO()
                    TINYTEXT -> TODO()
                    TEXT -> TODO()
                    MEDIUMTEXT -> TODO()
                    LONGTEXT -> TODO()
                    ENUM -> TODO()
                    SET -> TODO()
                    BOOL -> TODO()
                    BOOLEAN -> TODO()
                    SERIAL -> TODO()
                    FIXED -> TODO()
                    JSON -> TODO()
                }
            }
            nTuples.add(nTuple)
        }
        return Table(name = tableHeader.tableName, columns = tableHeader.columns, tuples = nTuples.toMutableList())
    }
}
