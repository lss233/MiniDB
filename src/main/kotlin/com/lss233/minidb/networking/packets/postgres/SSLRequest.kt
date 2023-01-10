package com.lss233.minidb.networking.packets.postgres

import io.netty.buffer.ByteBuf

class SSLRequest : IncomingPacket {
    private var requestCode : Int? = null

    override fun parse(buf: ByteBuf): IncomingPacket {
        requestCode = buf.readInt()
        return this
    }

}