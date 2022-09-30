package com.lss233.minidb.networking.packets

import io.netty.buffer.ByteBuf

class Terminate: PostgresSQLPacket, IncomingPacket {
    override fun parse(buf: ByteBuf): IncomingPacket {
        return this
    }

}
