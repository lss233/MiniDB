package com.lss233.minidb.networking.packets.mysql

import kotlin.reflect.KClass

enum class MessageType(val type: Byte, val `class`: KClass<out MySQLPacket>) {
    UnknownPacket(0, HandshakeV10::class),
    HandshakePacket(0, HandshakeV10::class),
    HandshakeResponsePacket(0, HandshakeResponse41::class),
    COM_INIT_DB(0x02, ChangeDatabase::class),
    COM_QUERY(0x03, RequestQuery::class),
    ;


    companion object {

        fun getType(type: Byte): MessageType {
            for (value in values()) {
                if(value.type == type) {
                    return value
                }
            }
            return UnknownPacket
        }
    }
}