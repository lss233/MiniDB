package com.lss233.minidb.networking

open class Session {
    var state = State.Startup
    var user: String? = null
    var database: String? = null
    val properties = HashMap<String, String>()
    var packetSequenceId = -1
    enum class State {
        Startup, Authenticating, Query, Terminated
    }
}