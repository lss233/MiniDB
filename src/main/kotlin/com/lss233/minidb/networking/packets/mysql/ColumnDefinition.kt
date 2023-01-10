package com.lss233.minidb.networking.packets.mysql

import com.lss233.minidb.engine.schema.Column
import com.lss233.minidb.networking.protocol.mysql.MySQLSession
import com.lss233.minidb.networking.utils.MySQLBufWrapper

class ColumnDefinition(
    private val column: Column
): OutgoingPacket {
    override fun write(out: MySQLBufWrapper, session: MySQLSession): OutgoingPacket {
        // catalog
        out.writeStringLengthEncoded("def")
        // schema name
        out.writeStringLengthEncoded(column.identifier.parent?.idText ?: "information_schema")
        // 	virtual table name
        out.writeStringLengthEncoded(column.tableName ?: "")
        // 	physical table name
        out.writeStringLengthEncoded(column.physicalTableName ?: "")
        // 	virtual column name
        out.writeStringLengthEncoded(column.name)
        // 	physical column name
        out.writeStringLengthEncoded(column.physicalColumnName ?: "")
        // [fixed] length of fields
        out.writeInt1(0x0C)
        // Charset
        out.writeInt2(0x21)
        // Length
        out.writeInt4(256)
        // field type
        // see https://dev.mysql.com/doc/dev/mysql-server/latest/field__types_8h.html
        out.writeInt1(253)
        // flags
        out.writeInt2(0x1001)
        // decimal
        out.writeInt1(0)

        // Not mentioned in doc
        out.writeInt2(0)
        return this
    }
}