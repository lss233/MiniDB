package com.lss233.minidb.networking.codec

import com.lss233.minidb.networking.Session
import com.lss233.minidb.networking.packets.mysql.IncomingPacket
import com.lss233.minidb.networking.packets.mysql.MessageType
import com.lss233.minidb.networking.protocol.mysql.MySQLSession
import com.lss233.minidb.networking.utils.MySQLBufWrapper
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import kotlin.reflect.full.createInstance

class MySQLDecoder(private val session: MySQLSession) : ByteToMessageDecoder() {
    private val BATCH_SIZE_MAX = 100000

    override fun decode(ctx: ChannelHandlerContext?, `in`: ByteBuf, out: MutableList<Any>) {
        while (`in`.readableBytes() >= 5 && out.size < BATCH_SIZE_MAX) {
            `in`.markReaderIndex()
            val mysqlBuf = MySQLBufWrapper(`in`)
            val len = mysqlBuf.readInt3()
            val sequenceId = mysqlBuf.readInt1()

            // 记录在 decode 阶段读的字节偏移
            val bytesRead = `in`.readerIndex()

            val hasMoreChunks = len == 16777215
            val mType = if(session.state == Session.State.Startup) {
                MessageType.HandshakeResponsePacket
            } else if(session.state == Session.State.Query){
                val commandType = `in`.readByte()
                MessageType.getType(commandType)
            } else {
                MessageType.UnknownPacket
            }

            println("-> type ${mType.name}(${mType.type.toInt().toChar()}) len $len, sequenceId $sequenceId")
            if (`in`.readableBytes() < len - `in`.readerIndex() + bytesRead) {
                println("   Incompleted message, try again.")
                `in`.resetReaderIndex()
                return
            }
            session.packetSequenceId = sequenceId

            val packet = mType.`class`.createInstance() as IncomingPacket
            out.add(packet.parse(mysqlBuf, session))
//
            `in`.readerIndex(`in`.writerIndex())
        }
    }
}
