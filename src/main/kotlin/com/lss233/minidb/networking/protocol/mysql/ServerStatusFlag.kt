package com.lss233.minidb.networking.protocol.mysql

enum class ServerStatusFlag(private val value: Int) {
    SERVER_STATUS_IN_TRANS(1),
    SERVER_STATUS_AUTOCOMMIT(2),
    SERVER_MORE_RESULTS_EXISTS(8),
    SERVER_QUERY_NO_GOOD_INDEX_USED(16),
    SERVER_QUERY_NO_INDEX_USED(32),
    SERVER_STATUS_CURSOR_EXISTS(64),
    SERVER_STATUS_LAST_ROW_SENT(128),
    SERVER_STATUS_DB_DROPPED(256),
    SERVER_STATUS_NO_BACKSLASH_ESCAPES(512),
    SERVER_STATUS_METADATA_CHANGED(1024),
    SERVER_QUERY_WAS_SLOW(2048),
    SERVER_PS_OUT_PARAMS(4096),
    SERVER_STATUS_IN_TRANS_READONLY(8192),
    SERVER_SESSION_STATE_CHANGED(1 shl 14);
    companion object {
        /**
         * Checks whether the provided number has capability
         */
        fun hasCapability(provided: Int, flag: ServerStatusFlag): Boolean =
            provided and flag.value > 0

        fun of(vararg capabilities: ServerStatusFlag): Int =
            capabilities.map { it.value }.reduce { acc, flags -> acc + flags }

    }
}