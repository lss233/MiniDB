package com.lss233.minidb.networking.protocol.mysql

/**
 * Values for the capabilities flag bitmask used by the MySQL protocol.
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__capabilities__flags.html">here</a>.
 */
enum class CapabilitiesFlags(val value: Long) {
    // new more secure passwords
    CLIENT_LONG_PASSWORD(1),

    // Found instead of affected rows
    CLIENT_FOUND_ROWS(2),

    // Get all column flags
    CLIENT_LONG_FLAG(4),

    // One can specify db on connect
    CLIENT_CONNECT_WITH_DB(8),

    // Don't allow database.table.column
    CLIENT_NO_SCHEMA(16),

    // Can use compression protocol
    CLIENT_COMPRESS(32),

    // Odbc client
    CLIENT_ODBC(64),

    // Can use LOAD DATA LOCAL
    CLIENT_LOCAL_FILES(128),

    // Ignore spaces before '('
    CLIENT_IGNORE_SPACE(256),

    // New 4.1 protocol
    CLIENT_PROTOCOL_41(512),

    // This is an interactive client
    CLIENT_INTERACTIVE(1024),

    // Switch to SSL after handshake
    CLIENT_SSL(2048),

    // IGNORE SIGPIPE
    CLIENT_IGNORE_SIGPIPE(4096),

    // Client knows about transactions
    CLIENT_TRANSACTIONS(8192),

    // DEPRECATED: Old flag for 4.1 protocol
    CLIENT_RESERVED(16384),

    // DEPRECATED: Old flag for 4.1 authentication.
    CLIENT_SECURE_CONNECTION(32768),

    // Enable/disable multi-stmt support
    CLIENT_MULTI_STATEMENTS(1 shl 16),

    // Enable/disable multi-results
    CLIENT_MULTI_RESULTS(1 shl 17),

    // Multi-results and OUT parameters in PS-protocol.
    CLIENT_PS_MULTI_RESULTS(1 shr 18),

    // Client supports plugin authentication.
    CLIENT_PLUGIN_AUTH(1 shl 19),

    // Client supports connection attributes.
    CLIENT_CONNECT_ATTRS(1 shl 20),

    // Enable authentication response packet to be larger than 255 bytes.
    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA(1 shl 21),

    // Don't close the connection for a user account with expired password.
    CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS(1 shl 22),

    // Capable of handling server state change information.
    CLIENT_SESSION_TRACK(1 shl 23),

    // Client no longer needs EOF_Packet and will use OK_Packet instead.
    CLIENT_DEPRECATE_EOF(1 shl 24),

    // The client can handle optional metadata information in the resultset.
    CLIENT_OPTIONAL_RESULTSET_METADATA(1 shl 25),

    // Compression protocol extended to support zstd compression method.
    CLIENT_ZSTD_COMPRESSION_ALGORITHM(1 shl 26),

    // Support optional extension for query parameters into the COM_QUERY and COM_STMT_EXECUTE packets.
    CLIENT_QUERY_ATTRIBUTES(1 shl 27),

    // Support Multi factor authentication.
    MULTI_FACTOR_AUTHENTICATION(1 shl 28),

    // This flag will be reserved to extend the 32bit capabilities structure to 64bits.
    CLIENT_CAPABILITY_EXTENSION(1 shl 29),

    // Verify server certificate.
    CLIENT_SSL_VERIFY_SERVER_CERT(1 shl 30),

    // Don't reset the options after an unsuccessful connect.
    CLIENT_REMEMBER_OPTIONS(1 shl 31),
    ;

    companion object {
        /**
         * Checks whether the provided number has capability
         */
        fun hasCapability(provided: Long, flag: CapabilitiesFlags): Boolean =
            provided and flag.value > 0

        fun of(vararg capabilities: CapabilitiesFlags): Long =
            capabilities.map { it.value }.reduce { acc, flags -> acc + flags }

        fun getCapabilities(provided: Long): Array<CapabilitiesFlags> =
            values().filter { flag -> hasCapability(provided, flag) }
                .toTypedArray()

    }
}
