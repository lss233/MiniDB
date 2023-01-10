package com.lss233.minidb.networking.protocol.mysql

import com.lss233.minidb.networking.Session

class MySQLSession: Session() {
    var clientFlags: Long = 0
}