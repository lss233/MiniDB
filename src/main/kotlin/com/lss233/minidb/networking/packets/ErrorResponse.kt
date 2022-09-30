package com.lss233.minidb.networking.packets

import io.netty.buffer.ByteBuf
import java.nio.charset.StandardCharsets

class ErrorResponse: PostgresSQLPacket, OutgoingPacket {
    var type: Int = 'C'.code
    var message: String = ""
    override fun write(buf: ByteBuf): OutgoingPacket {
        buf.writeInt(type)
        buf.writeCharSequence(message, StandardCharsets.UTF_8)
        return this
    }

}
