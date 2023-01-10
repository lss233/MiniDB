package com.lss233.minidb.networking.packets.mysql

import com.lss233.minidb.networking.protocol.mysql.CapabilitiesFlags
import com.lss233.minidb.networking.protocol.mysql.MySQLSession
import com.lss233.minidb.networking.utils.MySQLBufWrapper

class HandshakeResponse41: IncomingPacket {
    var clientFlag: Long? = null
    var maxPacketSize: Int? = null
    var characterSet: Int? = null
    var username: String? = null
    var authResponse: String? = null
    var database: String? = null
    var clientPluginName: String? = null
    val connectAttrs = HashMap<String, String>()
    var compressionLevel: Int? = null

    override fun parse(`in`: MySQLBufWrapper, session: MySQLSession): IncomingPacket {
        clientFlag = `in`.readInt4().toLong()
        maxPacketSize = `in`.readInt4()
        characterSet = `in`.readInt1()
        `in`.buf.skipBytes(23)
        username = `in`.readStringNullTerminated()
        if(CapabilitiesFlags.hasCapability(clientFlag!!, CapabilitiesFlags.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)) {
            authResponse = `in`.readStringLengthEncoded()
        } else {
            val authResponseLength = `in`.readInt1()
            authResponse = `in`.readStringVariableLength(authResponseLength)
        }
        if(CapabilitiesFlags.hasCapability(clientFlag!!, CapabilitiesFlags.CLIENT_CONNECT_WITH_DB)) {
            database = `in`.readStringNullTerminated()
        }

        if(CapabilitiesFlags.hasCapability(clientFlag!!, CapabilitiesFlags.CLIENT_PLUGIN_AUTH)) {
            clientPluginName = `in`.readStringNullTerminated()
        }
        if(`in`.buf.readableBytes() > 0) {
            if(CapabilitiesFlags.hasCapability(clientFlag!!, CapabilitiesFlags.CLIENT_CONNECT_ATTRS)) {
                val attrAmount = `in`.readIntLengthEncoded()

                for (i in 1.rangeTo(attrAmount)) {
                    val key = `in`.readStringLengthEncoded()
                    val value = `in`.readStringLengthEncoded()
                    connectAttrs[key] = value
                }
            }

        }
        if(`in`.buf.readableBytes() > 0) {
            compressionLevel = `in`.readInt1()
        }


        return this
    }
}