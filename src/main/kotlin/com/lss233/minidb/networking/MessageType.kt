package com.lss233.minidb.networking

import com.lss233.minidb.networking.packets.SSLRequest
import com.lss233.minidb.networking.packets.StartupMessage

enum class MessageType(type: Byte) {
    SSLRequest(0x0FE.toByte()),

    StartupMessage(0x00.toByte()),

    AuthenticationOk('R'.code.toByte()),

    NoticeResponse('N'.code.toByte()),

    ErrorResponse('E'.code.toByte()),

    AuthRequest('R'.code.toByte()),

    ParameterStatus('S'.code.toByte()),

    BackendKeyData('K'.code.toByte()),

    ReadyForQuery('Z'.code.toByte()),

    CommandComplete('C'.code.toByte()),

    DataRow('D'.code.toByte()),

    CopyBothResponse('W'.code.toByte()),

    CopyData('d'.code.toByte()),

    CopyDone('c'.code.toByte()),

    RowDescription('T'.code.toByte()),

    NotificationResponse('A'.code.toByte()),

    UnknownPacket(0xFF.toByte()),

    Query('Q'.code.toByte()),
    EmptyQueryResponse('I'.code.toByte())
    ;

    var type: Byte

    init {
        this.type = type
    }


    companion object {

        fun getType(type: Byte): MessageType {
            for (value in values()) {
                if(value.type == type) {
                    return value
                }
            }
            val ret = UnknownPacket
            ret.type = type
            return ret
        }
    }
}