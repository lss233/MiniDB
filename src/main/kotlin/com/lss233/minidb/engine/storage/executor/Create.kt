package com.lss233.minidb.engine.storage.executor

import cn.hutool.core.io.FileUtil
import com.lss233.minidb.engine.config.DBConfig
import com.lss233.minidb.engine.config.DbStorageConfig
import com.lss233.minidb.engine.storage.struct.DbTableField
import com.lss233.minidb.engine.storage.type.TypeLen
import com.lss233.minidb.utils.TypeUtil
import miniDB.parser.ast.fragment.ddl.datatype.DataType
import miniDB.parser.ast.stmt.ddl.DDLCreateTableStatement
import miniDB.parser.recognizer.SQLParserDelegate
import java.io.File

/**
 * 工作流程
 * 检查库名，检查表名合法性。
 * check
 * 逐个生成表文件
 * 生成库文件
 */
class Create {

    private val sql = "create db_test(uid int,name varchar 20,birth datetime)primary key uid;"

    private var dbName: String? = null

    private val fields = ArrayList<DbTableField>()

    /**
     * 工作流程
     * 解析出db名，解析字段名生成字段列表
     * 生成物理存储计划
     * 调用存储
     */
    fun doCreate() {
        // todo  这里记得把库的信息存储到文件里。
        FileUtil.touch(File(DBConfig.DB_ROOT_PATH + dbName + DBConfig.TABLE_SUFFIX))
    }

    /**
     * 模拟解析SQL语句创建表的内容，进行表的创建
     */
    private fun getDbFields(sql: String) {
        val sqlTest = "CREATE TABLE `Test` ( Id_P int,LastName varchar(255),FirstName varchar(255),Address varchar(255),City varchar(255))"
        // 通过解析SQL后得到建表的实体类
        val ast = SQLParserDelegate.parse(sqlTest) as DDLCreateTableStatement

        println("解析：创建表:" + ast.table.idText)

        // TODO 以上解析结束，接下来开始生成库信息文件 表信息文件

    }


    private fun setTypeAndByteLenByStr(type: String, fieldLen: String, fieldPO: DbTableField) {
        var byteLen = 1
        //判断一下是不是存储长度可变的类型  char 或者 varchar
        if (TypeUtil.isVarLenType(type)) {
            byteLen = fieldLen.toInt() * 8 //换算成底层存储的byte长度
        }
        // TODO 数据类型需要合并AST当中的
        when (type) {
            "int" -> {
                fieldPO.type = DataType.DataTypeName.INT
                fieldPO.byteLen = TypeLen.INTEGER
            }

            "BIGINT" -> {
                fieldPO.type = DataType.DataTypeName.BIGINT
                fieldPO.byteLen = TypeLen.BIGINT
            }

            "CHAR" -> {
                fieldPO.type = DataType.DataTypeName.CHAR
                fieldPO.byteLen = byteLen
            }

            "VARCHAR" -> {
                fieldPO.type = DataType.DataTypeName.VARCHAR
                fieldPO.byteLen = byteLen
            }

            "DATA_TIME" -> {
                fieldPO.type = DataType.DataTypeName.DATETIME
                fieldPO.byteLen = TypeLen.DATA_TIME
            }

            "TIME_STAMP" -> {
                fieldPO.type = DataType.DataTypeName.TIMESTAMP
                fieldPO.byteLen = TypeLen.TIME_STAMP
            }
        }
    }

    /**
     * 这里存储的顺序是：
     * 删除检测位、插入时间戳、事务id、读标记、写标记、{长度位、长度位、。。。}、{数据、数据、数据。。。}
     * 计算存储这么一条信息需要的长度
     * @return 长度
     */
    private fun computeLen(): Int {
        var total: Int = DbStorageConfig.getTotalByteLen()
        // 计算长度位：一个长度为用一个int存储 有多少列就存多少
        total += fields.size * 32
        // 这里计算一下数据位
        for (field in fields)
            total += field.byteLen
        return total
    }
}