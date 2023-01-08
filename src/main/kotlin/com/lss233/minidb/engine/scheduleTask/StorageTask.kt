package com.lss233.minidb.engine.scheduleTask

import com.lss233.minidb.engine.memory.Engine
import java.util.*


class StorageTask : TimerTask() {

    override fun run() {
        println("save action")
        Engine.dataStorage()
    }

}

//fun main() {
//    val task = StorageTask()
//    Timer().schedule(task, Date(), 1000)
//}
