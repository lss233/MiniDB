package com.lss233.minidb.networking.packets.postgres

import io.netty.buffer.ByteBuf

class AuthenticationOk: OutgoingPacket {
    private val success = 0;
    override fun write(buf: ByteBuf): OutgoingPacket {
        buf.writeInt(success)
        return this
    }
}