package com.lss233.minidb.networking.packets

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled

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