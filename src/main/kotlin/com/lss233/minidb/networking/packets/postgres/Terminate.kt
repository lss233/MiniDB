package com.lss233.minidb.networking.packets.postgres

import io.netty.buffer.ByteBuf

class Terminate: IncomingPacket {
    override fun parse(buf: ByteBuf): IncomingPacket {
        return this
    }

}
