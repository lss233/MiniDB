package com.lss233.minidb.networking.codec

import com.lss233.minidb.engine.Relation
import com.lss233.minidb.networking.packets.mysql.*
import com.lss233.minidb.networking.protocol.mysql.CapabilitiesFlags
import com.lss233.minidb.networking.protocol.mysql.MySQLSession
import com.lss233.minidb.networking.utils.MySQLBufWrapper
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder

class MySQLEncoder(private val session: MySQLSession) : MessageToByteEncoder<OutgoingPacket>() {
    override fun encode(ctx: ChannelHandlerContext?, msg: OutgoingPacket?, out: ByteBuf?) {

        val buf = MySQLBufWrapper(Unpooled.buffer())
        msg?.write(buf, session)

        // '?' 代表未知标识符
        val len = buf.buf.writerIndex()
        println("<- ${msg?.javaClass?.simpleName} len $len")

//        out?.writeByte(type)
        val outBuf = MySQLBufWrapper(out!!)
        outBuf.writeInt3(len)
        outBuf.writeInt1(++session.packetSequenceId)
        out?.writeBytes(buf.buf, buf.buf.writerIndex())
    }

}

/**
 * Text Resultset
 * https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
 */
class RelationToMySQLEncoder(private val session: MySQLSession) : MessageToByteEncoder<Relation>() {

    override fun encode(ctx: ChannelHandlerContext?, msg: Relation?, out: ByteBuf?) {
        msg?.let { relation: Relation -> run {
            val metadataFollows = true
            ctx?.write(
                TextResultsetIndicator(
                    metadataFollows = metadataFollows,
                    columnCount = relation.columns.size)
            )!!
            if(metadataFollows ||
                !CapabilitiesFlags.hasCapability(session.clientFlags, CapabilitiesFlags.CLIENT_OPTIONAL_RESULTSET_METADATA)
            ) {
                for (column in relation.columns) {
                    ctx.write(ColumnDefinition(column))
                }
            }
//            ctx.write(EOFPacket())

            if(!CapabilitiesFlags.hasCapability(session.clientFlags, CapabilitiesFlags.CLIENT_DEPRECATE_EOF)) {
                ctx.write(EOFPacket())
            }

            for(row in relation.rows) {
                ctx.write(TextResultsetRow(row))
            }
            // If error_process encountered, handle it in caughtException
            if(CapabilitiesFlags.hasCapability(session.clientFlags, CapabilitiesFlags.CLIENT_DEPRECATE_EOF)) {
                ctx.write(OKPacket(
                    affectedRows = 0,
                    lastInsertId = 0,
                    statusFlag = 0x0022,
                    warnings = 0,
                    info = "",
                    sessionStateInfo = "",
                    isEOF = true
                ))
            } else {
                ctx.write(EOFPacket())
            }
            ctx.flush()
        } }

    }

}
