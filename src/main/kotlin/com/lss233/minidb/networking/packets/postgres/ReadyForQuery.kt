package com.lss233.minidb.networking.packets.postgres

import io.netty.buffer.ByteBuf

class ReadyForQuery: OutgoingPacket {
    private val transactionStatus = 'I'.code;
    override fun write(buf: ByteBuf): OutgoingPacket {
        buf.writeByte(transactionStatus);
        return this
    }

}
