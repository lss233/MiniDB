package com.lss233.minidb.networking.packets.mysql

import com.lss233.minidb.networking.protocol.mysql.MySQLSession
import com.lss233.minidb.networking.utils.MySQLBufWrapper

interface MySQLPacket {
}
interface OutgoingPacket: MySQLPacket {
    fun write(`out`: MySQLBufWrapper, session: MySQLSession): OutgoingPacket
}
interface IncomingPacket: MySQLPacket {
    fun parse(`in`: MySQLBufWrapper, session: MySQLSession): IncomingPacket
}