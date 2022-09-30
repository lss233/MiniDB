package com.lss233.minidb.networking

import com.lss233.minidb.networking.codec.PostgreSQLDecoder
import com.lss233.minidb.networking.codec.PostgreSQLEncoder
import com.lss233.minidb.networking.handler.TerminateHandler
import com.lss233.minidb.networking.handler.query.QueryHandler
import com.lss233.minidb.networking.handler.startup.SSLRequestRejectHandler
import com.lss233.minidb.networking.handler.startup.StartupMessageHandler
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel

class NettyServerInitializer() : ChannelInitializer<SocketChannel>() {

    @Throws(Exception::class)
    public override fun initChannel(ch: SocketChannel) {
        val session = Session()
        val pipeline = ch.pipeline()
        pipeline.addLast(PostgreSQLDecoder(session), PostgreSQLEncoder(session))
        pipeline.addLast(SSLRequestRejectHandler(session), StartupMessageHandler(session))
        pipeline.addLast(QueryHandler(session))
        pipeline.addLast(TerminateHandler(session))
    }
}
