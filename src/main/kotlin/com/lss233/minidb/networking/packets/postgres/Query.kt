package com.lss233.minidb.networking.packets.postgres

import io.netty.buffer.ByteBuf
import java.nio.charset.StandardCharsets

class Query: IncomingPacket {
    var queryString: String? = null;
    override fun parse(buf: ByteBuf): IncomingPacket {
//        val query = buf.toString(StandardCharsets.UTF_8)
//        buf.readerIndex(query.length)
        val query = buf.readCharSequence(buf.writerIndex() - buf.readerIndex() - 1, StandardCharsets.UTF_8)
        // postgres string ends with a \0
        queryString = query.toString()
        return this
    }

}