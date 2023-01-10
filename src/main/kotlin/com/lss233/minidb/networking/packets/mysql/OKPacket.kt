package com.lss233.minidb.networking.packets.mysql

import com.lss233.minidb.networking.protocol.mysql.CapabilitiesFlags
import com.lss233.minidb.networking.protocol.mysql.MySQLSession
import com.lss233.minidb.networking.protocol.mysql.ServerStatusFlag
import com.lss233.minidb.networking.utils.MySQLBufWrapper

class OKPacket(
    val affectedRows: Long = 0,
    val lastInsertId: Long = 0,
    val statusFlag: Int = 0,
    val warnings: Int = 0,
    val info: String = "",
    val sessionStateInfo: String? = "",
    val isEOF: Boolean = false
): OutgoingPacket {

    override fun write(out: MySQLBufWrapper, session: MySQLSession): OutgoingPacket {
        if(isEOF) {
            out.writeInt1(0xFE)
        } else {
            out.writeInt1(0x00)
        }
        out.writeIntLengthEncoded(affectedRows.toULong())
        out.writeIntLengthEncoded(lastInsertId.toULong())
        if(CapabilitiesFlags.hasCapability(session.clientFlags, CapabilitiesFlags.CLIENT_PROTOCOL_41)) {
            out.writeInt2(statusFlag)
            out.writeInt2(warnings)
        } else if(CapabilitiesFlags.hasCapability(session.clientFlags, CapabilitiesFlags.CLIENT_TRANSACTIONS)) {
            out.writeInt2(statusFlag)
        }
        if(CapabilitiesFlags.hasCapability(session.clientFlags, CapabilitiesFlags.CLIENT_SESSION_TRACK)) {
            out.writeStringLengthEncoded(info)
            if(ServerStatusFlag.hasCapability(statusFlag, ServerStatusFlag.SERVER_SESSION_STATE_CHANGED)) {
                out.writeStringLengthEncoded(sessionStateInfo!!)
            }
        } else {
            out.writeStringEOF(info)
        }

        return this
    }
}