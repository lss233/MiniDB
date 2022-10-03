package com.lss233.minidb.networking.packets

import io.netty.buffer.ByteBuf

interface PostgreSQLPacket {
}
interface OutgoingPacket: PostgreSQLPacket {
    fun write(buf: ByteBuf): OutgoingPacket
}
interface IncomingPacket: PostgreSQLPacket {
    fun parse(buf: ByteBuf): IncomingPacket
}