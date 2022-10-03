package com.lss233.minidb.networking.packets

import io.netty.buffer.ByteBuf
import java.nio.charset.StandardCharsets

class AuthenticationSASL(private val methods: List<String>): OutgoingPacket {
    override fun write(buf: ByteBuf): OutgoingPacket {
        for (method in methods) {
            buf.writeCharSequence(method, StandardCharsets.UTF_8)
        }
        buf.writeByte(0)
        return this
    }

}
