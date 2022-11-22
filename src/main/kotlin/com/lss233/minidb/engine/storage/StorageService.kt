package com.lss233.minidb.engine.storage

import cn.hutool.core.io.FileUtil
import com.lss233.minidb.engine.NTuple
import com.lss233.minidb.engine.config.DBConfig
import com.lss233.minidb.engine.config.DbStorageConfig
import com.lss233.minidb.engine.memory.Table
import com.lss233.minidb.engine.schema.Column
import com.lss233.minidb.engine.storage.struct.TableField
import com.lss233.minidb.engine.storage.struct.TableHeader
import com.lss233.minidb.utils.ByteUtil
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import miniDB.parser.ast.fragment.ddl.datatype.DataType.DataTypeName.*
import java.nio.charset.Charset

class StorageService {

    companion object {

        const val TABLE_HEADER_SIZE = 16;

        const val PAGE_SIZE = 16;
    }

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

    fun getTable(dbName:String,tableName:String): ArrayList<NTuple> {
        // Open Byte File

        // Do One For Pages And Transform EntityCLass

        return ArrayList()
    }

    fun saveToTable(table:Table,nTuples: ArrayList<NTuple>) {

        var tableData:ByteArray




    }

        private fun buildStorageBytes(nTuples: ArrayList<NTuple>):ByteArray {
        val tableHeader = TableHeader()

        // Basic the first nTuple's columns form the table header info.
        val columnInfo = nTuples[0].columns
        for (item in columnInfo) {
            tableHeader.addTableFile(TableField(item.name, item.definition.dataType.typeName))
        }
        // After parsing all columns, we can calculate the length occupied by a column
        val tupleSize = tableHeader.getColumnStorageSize()

        // totalBytes = Table header size + NTuple_Count * (The sum of every type that exist)
        val totalBytes = DbStorageConfig.TABLE_HEADER_SIZE + nTuples.size * tupleSize

        val storageBytes = ByteArray(totalBytes)
        // Encoding tableHeader information
        val tableHeaderBytes = Json.encodeToString(tableHeader).toByteArray(Charset.defaultCharset())

        // Copy header info
        System.arraycopy(tableHeaderBytes, 0, totalBytes, 0, DbStorageConfig.TABLE_HEADER_SIZE)

        var realDataPos = DbStorageConfig.TABLE_HEADER_SIZE

        // copy body info
        for (tuple in nTuples) {
            // order by table header
            for (field in tableHeader.tableField!!) {
                val dataTemp = tuple[Column(field.colName)]
                when(field.dataTypeName) {
                    INT -> {
                        ByteUtil.arraycopy(storageBytes, realDataPos, ByteUtil.intToByte4(dataTemp as Int))
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
                        val str = dataTemp.toString().toByteArray(Charset.defaultCharset())
                        val strByteSize = str.size
                        System.arraycopy(str, 0, storageBytes, realDataPos, strByteSize)
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
}
