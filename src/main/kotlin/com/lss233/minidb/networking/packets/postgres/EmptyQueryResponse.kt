package com.lss233.minidb.networking.packets.postgres

import io.netty.buffer.ByteBuf

class EmptyQueryResponse: OutgoingPacket {
    override fun write(buf: ByteBuf): OutgoingPacket {
        return this
    }
}