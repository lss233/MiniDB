package com.lss233.minidb.utils

import java.io.IOException
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes

object Misc {
    @Throws(IOException::class)
    fun rmDir(path: Path) {
        Files.walkFileTree(path, object : SimpleFileVisitor<Path>() {
            @Throws(IOException::class)
            override fun visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult {
                Files.delete(file)
                return FileVisitResult.CONTINUE
            }

            @Throws(IOException::class)
            override fun postVisitDirectory(dir: Path, exc: IOException): FileVisitResult {
                Files.delete(dir)
                return FileVisitResult.CONTINUE
            }
        })
    }

    @Throws(IOException::class)
    fun rmDir(name: String) {
        val path = Paths.get(name)
        rmDir(path)
    }
}