package com.lss233.minidb.utils

import io.netty.buffer.ByteBuf
import java.nio.charset.StandardCharsets

class BufUtil {
    companion object {
        fun nextString(buf: ByteBuf): String? {
            var i = 1
            while(buf.writerIndex() >= buf.readerIndex() + i) {
                if(buf.getByte(buf.readerIndex() + i).toInt() == 0) {
                    break
                }
                i++;
            }
            if(i > 1) {
                val startPos = buf.readerIndex()
                buf.readerIndex(startPos + i + 1)
                return buf.getCharSequence(startPos, i, StandardCharsets.UTF_8).toString()
            }
            return null
        }
    }

}
