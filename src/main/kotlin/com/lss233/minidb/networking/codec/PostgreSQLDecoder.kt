package com.lss233.minidb.networking.codec

import com.lss233.minidb.networking.packets.postgres.MessageType
import com.lss233.minidb.networking.Session
import com.lss233.minidb.networking.packets.postgres.*
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder

class PostgreSQLDecoder(private val session: Session) : ByteToMessageDecoder() {
    private val BATCH_SIZE_MAX = 100000

    override fun decode(ctx: ChannelHandlerContext?, `in`: ByteBuf, out: MutableList<Any>) {
        while (`in`.readableBytes() >= 5 && out.size < BATCH_SIZE_MAX) {
            `in`.markReaderIndex()
            val mType = if(session.state == Session.State.Startup) {
                val position = `in`.readerIndex()
                val length = `in`.readInt()
                val magicNumber = `in`.readInt()
                `in`.readerIndex(position)
                if(length == 8 && magicNumber == 80877103) {
                    MessageType.SSLRequest
                } else {
                    MessageType.StartupMessage
                }
            } else {
                MessageType.getType(`in`.readByte())
            }

            val len = `in`.readInt()
            val length = len - `in`.readerIndex()
            println("-> type ${mType.name}(${mType.type.toInt().toChar()}) len $len")
            if (`in`.readableBytes() < length) {
                println("   Uncompleted message, try again.")
                `in`.resetReaderIndex()
                return
            }

            val packet: IncomingPacket? = parse(mType, `in`);
            packet?.let { out.add(it) }
//
            `in`.readerIndex(`in`.writerIndex())
        }
    }

    companion object {
            fun parse(type: MessageType?, payload: ByteBuf): IncomingPacket? {
                return when(type) {
                    MessageType.SSLRequest -> SSLRequest().parse(payload)
                    MessageType.StartupMessage -> StartupMessage().parse(payload)
                    MessageType.Query -> Query().parse(payload)
                    MessageType.Terminate -> Terminate().parse(payload)
                    else -> null
                }
            }

    }

}
