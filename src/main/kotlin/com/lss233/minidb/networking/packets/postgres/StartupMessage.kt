package com.lss233.minidb.networking.packets.postgres

import com.lss233.minidb.utils.BufUtil
import io.netty.buffer.ByteBuf

class StartupMessage : IncomingPacket {
    private var protocolVersion : Int? = null
    var parameters = HashMap<String, String>()

    override fun parse(buf: ByteBuf): IncomingPacket {
        protocolVersion = buf.readInt()
        println("Begin reading startupMessage")

        var key = BufUtil.nextString(buf)
        while(key != null) {
            parameters[key] = BufUtil.nextString(buf)!!
            key = BufUtil.nextString(buf)
        }
        return this
    }

}