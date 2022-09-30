package com.lss233.minidb.networking.packets

import io.netty.buffer.ByteBuf

class ReadyForQuery: PostgresSQLPacket, OutgoingPacket {
    private val transactionStatus = 'I'.code;
    override fun write(buf: ByteBuf): OutgoingPacket {
        buf.writeByte(transactionStatus);
        return this
    }

}
