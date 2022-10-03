package com.lss233.minidb.networking.packets

import io.netty.buffer.ByteBuf
import java.nio.charset.StandardCharsets

class NotificationResponse: IncomingPacket {
    var processId: Int? = null;
    var channel: String? = null;
    var payload: String? = null;

    override fun parse(buf: ByteBuf): IncomingPacket {
        processId = buf.readInt()
        channel = buf.toString(StandardCharsets.UTF_8)
        payload = buf.toString(StandardCharsets.UTF_8)
        return this
    }
}