package com.lss233.minidb.networking

import com.lss233.minidb.networking.codec.MySQLDecoder
import com.lss233.minidb.networking.codec.MySQLEncoder
import com.lss233.minidb.networking.codec.RelationToMySQLEncoder
import com.lss233.minidb.networking.handler.mysql.command.ChangeDatabaseHandler
import com.lss233.minidb.networking.handler.mysql.command.QueryHandler
import com.lss233.minidb.networking.handler.mysql.handshake.InitialHandshakeHandler
import com.lss233.minidb.networking.packets.mysql.HandshakeV10
import com.lss233.minidb.networking.protocol.mysql.CapabilitiesFlags
import com.lss233.minidb.networking.protocol.mysql.MySQLSession
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel

class MySQLProtocolInitializer() : ChannelInitializer<SocketChannel>() {

    @Throws(Exception::class)
    public override fun initChannel(ch: SocketChannel) {
        val session = MySQLSession()
        val pipeline = ch.pipeline()
        pipeline.addLast(MySQLDecoder(session), MySQLEncoder(session))
        pipeline.addLast(RelationToMySQLEncoder(session))
        pipeline.addLast(InitialHandshakeHandler(session))
        pipeline.addLast(QueryHandler(session))
        pipeline.addLast(ChangeDatabaseHandler(session))
        val serverCapability = CapabilitiesFlags.of(
            CapabilitiesFlags.CLIENT_LONG_PASSWORD,
            CapabilitiesFlags.CLIENT_FOUND_ROWS,
            CapabilitiesFlags.CLIENT_LONG_FLAG,
            CapabilitiesFlags.CLIENT_NO_SCHEMA,
            CapabilitiesFlags.CLIENT_COMPRESS,
            CapabilitiesFlags.CLIENT_LOCAL_FILES,
            CapabilitiesFlags.CLIENT_IGNORE_SPACE,
            CapabilitiesFlags.CLIENT_PROTOCOL_41,
            CapabilitiesFlags.CLIENT_CONNECT_WITH_DB,
            CapabilitiesFlags.CLIENT_PLUGIN_AUTH,

            CapabilitiesFlags.CLIENT_SECURE_CONNECTION,
            CapabilitiesFlags.CLIENT_DEPRECATE_EOF,
            CapabilitiesFlags.CLIENT_MULTI_STATEMENTS,
        )
        session.clientFlags = serverCapability
        session.properties["lower_case_file_system"] = "OFF"
        session.properties["lower_case_table_names"] = "0"

        val handshake = HandshakeV10()
        handshake.serverVersion = "5.5.5-1.0.0-MiniDB"
        handshake.connectionId = connectionId++
        handshake.scramble = "ran&nz;Tsf*9#>Q[Uvvm"
        handshake.capabilityFlags = serverCapability
        // utf8mb3_general_ci
        handshake.characterSet = 0x2d
        handshake.statusFlags = 0x0002
        handshake.authPluginName = "mysql_native_password"

        ch.writeAndFlush(handshake)

    }

    companion object {
        private var connectionId = 1
    }
}