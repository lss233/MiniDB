package com.lss233.minidb.networking.packets.postgres

import io.netty.buffer.ByteBuf

class DataRow(val columnData: Array<ColumnData>): OutgoingPacket {
    override fun write(buf: ByteBuf): OutgoingPacket {
        buf.writeShort(columnData.size)
        for(col in columnData) {
            if(col.bytes == null) {
                buf.writeInt(-1)
            } else {
                buf.writeInt(col.bytes.size)
                buf.writeBytes(col.bytes)
            }
        }
        return this
    }
    class ColumnData(val bytes: ByteArray?) {
    }
}