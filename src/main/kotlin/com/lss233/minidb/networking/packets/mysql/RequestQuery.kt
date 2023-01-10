package com.lss233.minidb.networking.packets.mysql

import com.lss233.minidb.networking.protocol.mysql.CapabilitiesFlags
import com.lss233.minidb.networking.protocol.mysql.MySQLSession
import com.lss233.minidb.networking.utils.MySQLBufWrapper
import java.nio.charset.StandardCharsets

class RequestQuery: IncomingPacket {
    var parameters = HashMap<String, String>()
//    var parameterCount: Long = 0
    var parameterSetCount: Long = 0
    var nullBitmap: ByteArray? = null
    var newParamsBindFlag: Int = 0
    var paramTypeAndFlag: Int = 0
    var parameterName: String = ""
    var parameterValue: String = ""
    var query: String = ""

    override fun parse(`in`: MySQLBufWrapper, session: MySQLSession): IncomingPacket {
        if(CapabilitiesFlags.hasCapability(session.clientFlags, CapabilitiesFlags.CLIENT_QUERY_ATTRIBUTES)) {
            val parameterCount = `in`.readIntLengthEncoded()
            parameterSetCount = `in`.readIntLengthEncoded()
            if(parameterCount > 0) {
                nullBitmap = `in`.readStringLengthEncoded().toByteArray(StandardCharsets.UTF_8)
                newParamsBindFlag = `in`.readInt1()
                if(newParamsBindFlag == 1) {
                    val parameterName = `in`.readStringLengthEncoded()
                    val parameterValue = `in`.readStringLengthEncoded()
                    parameters[parameterName] = parameterValue
                }
            }
        }
        query = `in`.readStringEOF()
        return this
    }
}