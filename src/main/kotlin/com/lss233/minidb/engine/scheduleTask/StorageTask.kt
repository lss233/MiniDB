package com.lss233.minidb.engine.scheduleTask

import java.util.*


class StorageTask : TimerTask() {

    override fun run() {
        println("schedule task ===>")
    }

}

//fun main() {
//    val task = StorageTask()
//    Timer().schedule(task, Date(), 1000)
//}