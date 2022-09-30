package com.lss233.minidb.networking

class Session {
    var state = State.Startup
    enum class State {
        Startup, Query
    }
}