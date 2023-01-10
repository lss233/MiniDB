package com.lss233.minidb.networking.handler.postgres

import com.lss233.minidb.networking.Session
import com.lss233.minidb.networking.packets.postgres.Terminate
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler

class TerminateHandler(private val session: Session) : SimpleChannelInboundHandler<Terminate>(){
    override fun channelRead0(ctx: ChannelHandlerContext?, msg: Terminate?) {
        session.state = Session.State.Terminated
        ctx?.close()?.syncUninterruptibly()
    }

}
