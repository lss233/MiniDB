package com.lss233.minidb.networking.packets

import io.netty.buffer.ByteBuf
import java.nio.charset.StandardCharsets

class ParameterStatus(private val key: String, private val value: String): PostgresSQLPacket, OutgoingPacket {
    override fun write(buf: ByteBuf): OutgoingPacket {
        buf.writeCharSequence(key, StandardCharsets.UTF_8)
        buf.writeCharSequence(value, StandardCharsets.UTF_8)
        return this
    }
}