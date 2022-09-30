package com.lss233.minidb.networking.packets

import io.netty.buffer.ByteBuf
import java.nio.charset.StandardCharsets

class SSLRequest : IncomingPacket {
    private var requestCode : Int? = null

    override fun parse(buf: ByteBuf): IncomingPacket {
        requestCode = buf.readInt()
        return this
    }

}