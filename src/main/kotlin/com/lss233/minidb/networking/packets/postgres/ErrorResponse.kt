package com.lss233.minidb.networking.packets.postgres

import io.netty.buffer.ByteBuf
import java.nio.charset.StandardCharsets

class ErrorResponse: OutgoingPacket {
    var severity: String = "错误"
    var severityV: String = "ERROR"
    var sqlStateCode: String = "42703"
    var message: String = "An error has occur."
    var detail: String? = null
    var position: String? = "1"
    var file: String? = null
    var line: Int? = 1
    var routine: String? = null
    override fun write(buf: ByteBuf): OutgoingPacket {
        buf.writeByte('S'.code)
        buf.writeCharSequence(severity, StandardCharsets.UTF_8)
        buf.writeByte(0)

        buf.writeByte('V'.code)
        buf.writeCharSequence(severityV, StandardCharsets.UTF_8)
        buf.writeByte(0)


        buf.writeByte('C'.code)
        buf.writeCharSequence(sqlStateCode, StandardCharsets.UTF_8)
        buf.writeByte(0)

        buf.writeByte('M'.code)
        buf.writeCharSequence(message, StandardCharsets.UTF_8)
        buf.writeByte(0)

        if(detail != null) {
            buf.writeByte('D'.code)
            buf.writeCharSequence(detail, StandardCharsets.UTF_8)
            buf.writeByte(0)
        }

        if(position != null) {
            buf.writeByte('P'.code)
            buf.writeCharSequence(position, StandardCharsets.UTF_8)
            buf.writeByte(0)
        }

        buf.writeByte('F'.code)
        buf.writeCharSequence(file, StandardCharsets.UTF_8)
        buf.writeByte(0)

        buf.writeByte('R'.code)
        buf.writeCharSequence(routine ?: "unknownRoutine", StandardCharsets.UTF_8)
        buf.writeByte(0)

        buf.writeByte('L'.code)
        buf.writeCharSequence(line.toString(), StandardCharsets.UTF_8)
        buf.writeByte(0)
        buf.writeByte(0)

        return this
    }

}
