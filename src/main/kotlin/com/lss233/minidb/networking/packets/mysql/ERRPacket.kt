package com.lss233.minidb.networking.packets.mysql

import com.lss233.minidb.networking.protocol.mysql.CapabilitiesFlags
import com.lss233.minidb.networking.protocol.mysql.MySQLSession
import com.lss233.minidb.networking.utils.MySQLBufWrapper

class ERRPacket(
    private val errorCode: Int,
    private val sqlStateMarker: String,
    private val sqlState: String,
    private val errorMessage: String
): OutgoingPacket {
    override fun write(out: MySQLBufWrapper, session: MySQLSession): OutgoingPacket {
        out.writeInt1(0xFF)
        out.writeInt2(errorCode)
        if(CapabilitiesFlags.hasCapability(session.clientFlags, CapabilitiesFlags.CLIENT_PROTOCOL_41)) {
            out.writeStringFixedLength(sqlStateMarker, 1)
            out.writeStringFixedLength(sqlState, 5)
        }
        out.writeStringEOF(errorMessage)
        return this
    }
}