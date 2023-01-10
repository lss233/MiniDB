package com.lss233.minidb.networking.packets.mysql

import com.lss233.minidb.networking.protocol.mysql.MySQLSession
import com.lss233.minidb.networking.utils.MySQLBufWrapper

class RequestShowFields: IncomingPacket {
    var table: String? = null
    var wildcard: String? = null
    override fun parse(`in`: MySQLBufWrapper, session: MySQLSession): IncomingPacket {
        table = `in`.readStringNullTerminated()
        wildcard = `in`.readStringEOF()
        return this
    }
}