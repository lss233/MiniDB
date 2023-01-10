package com.lss233.minidb.networking.packets.mysql

import com.lss233.minidb.networking.protocol.mysql.CapabilitiesFlags
import com.lss233.minidb.networking.protocol.mysql.MySQLSession
import com.lss233.minidb.networking.utils.MySQLBufWrapper

class EOFPacket(
    private val warnings: Int = 0,
    private val statusFlags: Int = 0x0200
): OutgoingPacket {
    override fun write(out: MySQLBufWrapper, session: MySQLSession): OutgoingPacket {
        out.writeInt1(0xFE)
        if(CapabilitiesFlags.hasCapability(session.clientFlags, CapabilitiesFlags.CLIENT_RESERVED)) {
            out.writeInt2(warnings)
            out.writeInt2(statusFlags)
        }
        return this
    }
}