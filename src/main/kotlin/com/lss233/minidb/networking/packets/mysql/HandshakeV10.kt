package com.lss233.minidb.networking.packets.mysql

import com.lss233.minidb.networking.protocol.mysql.CapabilitiesFlags
import com.lss233.minidb.networking.protocol.mysql.MySQLSession
import com.lss233.minidb.networking.utils.MySQLBufWrapper
import java.nio.charset.StandardCharsets

/**
 * Protocol::HandshakeV10
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html">here</a>
 */
class HandshakeV10: OutgoingPacket {
    var protocolVersion: Int = 10
    var serverVersion: String? = null
    var connectionId: Int? = null
    var scramble: String? = null
    var capabilityFlags: Long? = null
    var characterSet: Int? = null
    var statusFlags: Int? = null
    var authPluginName: String? = null


    override fun write(`out`: MySQLBufWrapper, session: MySQLSession): OutgoingPacket {
        `out`.writeInt1(protocolVersion)
        `out`.writeStringNullTerminated(serverVersion!!)
        `out`.writeInt4(connectionId!!)

        val encodedScramble = scramble?.toByteArray(StandardCharsets.UTF_8)
        `out`.buf.writeBytes(encodedScramble?.copyOfRange(0, 8))

        `out`.writeInt1(0)

        // lower 2 bytes of the capabilityFlags
        `out`.writeInt2((capabilityFlags!! and 0xFFFF).toInt())

        `out`.writeInt1(characterSet!!)
        `out`.writeInt2(statusFlags!!)
        // higher 2 bytes of the capabilityFlags
        `out`.writeInt2((capabilityFlags!! shr 16).toInt())

        if(CapabilitiesFlags.hasCapability(capabilityFlags!!, CapabilitiesFlags.CLIENT_PLUGIN_AUTH)) {
             `out`.writeInt1(scramble?.length?.plus(1) ?: 0)
        } else {
            `out`.writeInt1(0)
        }

        // 10 bytes of 0
        for (i in 0..9) {
            `out`.writeInt1(0)
        }
        if(encodedScramble?.size!! > 8) {
            `out`.writeStringNullTerminated(encodedScramble.copyOfRange(8, encodedScramble.size.coerceAtLeast(20)).toString(StandardCharsets.UTF_8))
        }

        if(CapabilitiesFlags.hasCapability(capabilityFlags!!, CapabilitiesFlags.CLIENT_PLUGIN_AUTH)) {
            `out`.writeStringNullTerminated(authPluginName!!)
        }

        return this
    }
}