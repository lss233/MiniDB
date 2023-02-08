package com.lss233.minidb.utils

import java.io.IOException
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes

class Misc {

    companion object {
        private fun rmDir(path: Path) {
            Files.walkFileTree(path, object : SimpleFileVisitor<Path>() {
                override fun visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult {
                    Files.delete(file)
                    return FileVisitResult.CONTINUE
                }

                override fun postVisitDirectory(dir: Path, exc: IOException): FileVisitResult {
                    Files.delete(dir)
                    return FileVisitResult.CONTINUE
                }
            })
        }

        fun rmDir(name: String) {
            val path = Paths.get(name)
            rmDir(path)
        }
    }
}