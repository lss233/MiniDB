package com.lss233.minidb.networking.packets.mysql

import com.lss233.minidb.networking.protocol.mysql.MySQLSession
import com.lss233.minidb.networking.utils.MySQLBufWrapper

class ChangeDatabase: IncomingPacket {
    var database: String? = null
    override fun parse(`in`: MySQLBufWrapper, session: MySQLSession): IncomingPacket {
        database = `in`.readStringEOF()
        return this
    }
}