package com.lss233.minidb.engine.memory.internal.catalog

import com.lss233.minidb.engine.Relation
import com.lss233.minidb.engine.memory.Database
import com.lss233.minidb.engine.memory.Engine
import com.lss233.minidb.engine.memory.View
import com.lss233.minidb.engine.schema.Column
import miniDB.parser.ast.fragment.ddl.datatype.DataType

/**
 * pg_database view
 */
class DatabaseView(database: Database) : PostgresCatalogView(database) {

    override fun getColumns() : MutableList<Column>
            = mutableListOf(
        Column("oid", DataType.DataTypeName.INT),
        Column("datname", DataType.DataTypeName.CHAR),
        Column("datdba", DataType.DataTypeName.CHAR),
        Column("encoding", DataType.DataTypeName.CHAR),
        Column("datlocprovider", DataType.DataTypeName.CHAR),
        Column("datistemplate", DataType.DataTypeName.CHAR),
        Column("datallowconn", DataType.DataTypeName.CHAR),
        Column("datconnlimit", DataType.DataTypeName.CHAR),
        Column("dattablespace", DataType.DataTypeName.CHAR),
        Column("datcollate", DataType.DataTypeName.CHAR),
        Column("datctype", DataType.DataTypeName.CHAR),
        Column("datacl", DataType.DataTypeName.CHAR)
    )

    override fun generateData(): MutableList<Array<Any>>
        = Engine.getDatabase()
            .entries
            .mapIndexed { index: Int, ( name: String, db: Database) ->
                arrayOf<Any>(
                    index + 1, // oid
                    name, // datname
                    db.dba, // datdba
                    db.encoding, // encoding
                    db.locProvider, // locprovider
                    false, // istemplate
                    db.allowConn, // allowConn
                    db.connLimit, // connlimit
                    "1", // tablespace
                    "1", // collate
                    "1", // ctype
                    "[]")
            }.toMutableList()
}