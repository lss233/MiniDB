package miniDB

import com.lss233.minidb.engine.storage.executor.Create
import com.lss233.minidb.engine.storage.page.Page
import com.lss233.minidb.utils.FileUtil
import java.io.*
import java.nio.charset.Charset

fun main(args: Array<String>) {

    val create = Create()

    create.doCreate()

    // Try adding program arguments via Run/Debug configuration.
    // Learn more about running applications: https://www.jetbrains.com/help/idea/running-applications.html.
    println("Program arguments: ${args.joinToString()}")
}

fun test() {

    val currentDir = System.getProperty(".") + "\\out"

    val file = FileUtil("test")

    val s = "\n这是一个字符串类型数据，我要把这段文字转换成比特流存储。\n"

    file.appendWriteFile("haha",s.toByteArray())

    val stringData = file.readFileAsString("haha")
    println(stringData)

//    file.writeText("呵呵呵哈哈哈")
//    println(file.readText())
//
//    file.writeBytes(byteArrayOf(12, 56, 83, 57))
//    println(file.readText())
//
//    //追加方式写入字节或字符
//    file.appendBytes(byteArrayOf(93, 85, 74, 93))
//    file.appendText("吼啊")
//    println(file.readText())

    //直接使用writer和outputstream
//    val writer: Writer = file.writer()
//    val outputStream: OutputStream = file.outputStream()
//    val printWriter: PrintWriter = file.printWriter()
}