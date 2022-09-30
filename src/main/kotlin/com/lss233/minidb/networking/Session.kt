package com.lss233.minidb.networking

class Session {
    var state = State.Startup
    var user: String? = null
    var database: String? = null
    val properties = HashMap<String, String>()
    enum class State {
        Startup, Authenticating, Query, Terminated
    }
}