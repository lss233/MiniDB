package com.lss233.minidb.networking.packets

import io.netty.buffer.ByteBuf

class StartupMessage : IncomingPacket {
    private var protocolVersion : Int? = null

    override fun parse(buf: ByteBuf): IncomingPacket {
        protocolVersion = buf.readInt()
        return this
    }

}