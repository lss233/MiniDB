package com.lss233.minidb.networking.packets.mysql

import com.lss233.minidb.networking.protocol.mysql.MySQLSession
import com.lss233.minidb.networking.utils.MySQLBufWrapper

class TextResultsetRow(private val row: Array<Any>): OutgoingPacket {
    override fun write(out: MySQLBufWrapper, session: MySQLSession): OutgoingPacket {
        for(field in row) {
            if(field == null) {
                out.buf.writeByte(0xFB)
            } else {
                out.writeStringLengthEncoded(field.toString())
            }
        }
        return this
    }
}