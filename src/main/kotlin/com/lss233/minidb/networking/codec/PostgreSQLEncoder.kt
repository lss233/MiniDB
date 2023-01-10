package com.lss233.minidb.networking.codec

import com.lss233.minidb.networking.packets.postgres.MessageType
import com.lss233.minidb.networking.Session
import com.lss233.minidb.networking.packets.postgres.OutgoingPacket
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder

class PostgreSQLEncoder(private val session: Session) : MessageToByteEncoder<OutgoingPacket>() {
    override fun encode(ctx: ChannelHandlerContext?, msg: OutgoingPacket?, out: ByteBuf?) {
        val mType = msg?.javaClass?.simpleName?.let { MessageType.valueOf(it) }

        val buf = Unpooled.buffer()
        msg?.write(buf)

        val type = mType?.type?.toInt() ?: '?'.code
        // '?' 代表未知标识符
        val len = buf.writerIndex() + 4
        println("<- ${msg?.javaClass?.simpleName}(${type.toChar()}) len $len")

        out?.writeByte(type)
        out?.writeInt(len)
        out?.writeBytes(buf, buf.writerIndex())
    }

}
