package com.lss233.minidb.networking.codec

import com.lss233.minidb.networking.MessageType
import com.lss233.minidb.networking.Session
import com.lss233.minidb.networking.packets.OutgoingPacket
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder

class PostgreSQLEncoder(private val session: Session) : MessageToByteEncoder<OutgoingPacket>() {
    override fun encode(ctx: ChannelHandlerContext?, msg: OutgoingPacket?, out: ByteBuf?) {
        println("<- " + msg?.javaClass?.simpleName)
        val mType = msg?.javaClass?.simpleName?.let { MessageType.valueOf(it) }

        val buf = Unpooled.buffer(1)
        msg?.write(buf)
//        val bytes = buf.array()
        out?.writeByte(mType?.type?.toInt() ?: 0)
        out?.writeInt(buf.writerIndex() + 4)
//        out?.writeInt(bytes.size)
        out?.writeBytes(buf, buf.writerIndex())
    }

}
