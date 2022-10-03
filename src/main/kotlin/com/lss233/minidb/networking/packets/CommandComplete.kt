package com.lss233.minidb.networking.packets

import io.netty.buffer.ByteBuf
import java.nio.charset.StandardCharsets

class CommandComplete(private val tag: String): OutgoingPacket {

    override fun write(buf: ByteBuf): OutgoingPacket {
        buf.writeCharSequence(tag, StandardCharsets.UTF_8)
        buf.writeByte(0)
        return this
    }
}