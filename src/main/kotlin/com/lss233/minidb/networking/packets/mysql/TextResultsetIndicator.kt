package com.lss233.minidb.networking.packets.mysql

import com.lss233.minidb.networking.protocol.mysql.CapabilitiesFlags
import com.lss233.minidb.networking.protocol.mysql.MySQLSession
import com.lss233.minidb.networking.utils.MySQLBufWrapper

class TextResultsetIndicator(
    private val metadataFollows: Boolean,
    private val columnCount: Int,
): OutgoingPacket {
    override fun write(out: MySQLBufWrapper, session: MySQLSession): OutgoingPacket {
        if(CapabilitiesFlags.hasCapability(session.clientFlags, CapabilitiesFlags.CLIENT_OPTIONAL_RESULTSET_METADATA)) {
            out.writeInt1(if(metadataFollows) 1 else 0)
        }
        out.writeIntLengthEncoded(columnCount.toULong())
        return this
    }
}