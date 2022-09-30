package com.lss233.minidb.networking.packets

import io.netty.buffer.ByteBuf

class AuthenticationOk: PostgresSQLPacket, OutgoingPacket{
    private val success = 0;
    override fun write(buf: ByteBuf): OutgoingPacket {
        buf.writeInt(success)
        return this
    }
}