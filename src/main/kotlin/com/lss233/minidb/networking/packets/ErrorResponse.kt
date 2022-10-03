package com.lss233.minidb.networking.packets

import io.netty.buffer.ByteBuf
import java.nio.charset.StandardCharsets

class ErrorResponse: OutgoingPacket {
    var type: Int = 'C'.code
    var message: String = ""
    override fun write(buf: ByteBuf): OutgoingPacket {
        buf.writeByte(type)
        buf.writeCharSequence(message, StandardCharsets.UTF_8)
        return this
    }

}
