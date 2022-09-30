package com.lss233.minidb.networking.packets

import io.netty.buffer.ByteBuf

interface PostgresSQLPacket {
}
interface OutgoingPacket {
    fun write(buf: ByteBuf): OutgoingPacket
}
interface IncomingPacket {
    fun parse(buf: ByteBuf): IncomingPacket
}