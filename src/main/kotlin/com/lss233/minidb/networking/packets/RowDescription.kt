package com.lss233.minidb.networking.packets

import io.netty.buffer.ByteBuf
import java.nio.charset.StandardCharsets

class RowDescription: OutgoingPacket {
    val rowData = ArrayList<RowData>()
    override fun write(buf: ByteBuf): OutgoingPacket {
        buf.writeShort(rowData.size)
        for (data in rowData) {
            data.write(buf)
        }
        return this
    }
    class RowData {
        var name: String? = null
        var objectId: Int = 0
        var attributeNumber: Short = 0
        var dataType: Int? = null
        var typeSize: Short? = 0
        var typeModifier: Int? = null
        var formatCode: Short? = null

        fun write(buf: ByteBuf): RowData {
            buf.writeCharSequence(name, StandardCharsets.UTF_8)
            buf.writeInt(objectId)
            buf.writeShort(attributeNumber.toInt())
            buf.writeInt(dataType!!)
            buf.writeShort(typeSize!!.toInt())
            buf.writeInt(typeModifier!!)
            buf.writeShort(formatCode!!.toInt())
            return this
        }
    }
}