package com.lss233.minidb.networking.utils

import io.netty.buffer.ByteBuf
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets

class MySQLBufWrapper(val buf: ByteBuf) {


    fun readStringNullTerminated(): String {
        var i = 1
        while(buf.writerIndex() >= buf.readerIndex() + i) {
            if(buf.getByte(buf.readerIndex() + i).toInt() == 0) {
                break
            }
            i++;
        }
        val startPos = buf.readerIndex()
        buf.readerIndex(startPos + i + 1)
        return buf.getCharSequence(startPos, i, StandardCharsets.UTF_8).toString()
    }


    fun readStringVariableLength(length: Int): String {
        return buf.readCharSequence(length, StandardCharsets.UTF_8).toString()
    }

    fun readStringLengthEncoded(): String {
        return buf.readCharSequence(readIntLengthEncoded().toInt(), StandardCharsets.UTF_8).toString()
    }

    fun readStringEOF(): String =
        buf.readCharSequence(buf.writerIndex() - buf.readerIndex(), StandardCharsets.UTF_8).toString()

    fun readIntLengthEncoded(): Long {
        val firstByte = buf.readByte().toUInt()
        if(firstByte == 0xFCu) {
            return readInt2().toLong()
        }
        if(firstByte == 0xFDu) {
            return readInt3().toLong()
        }
        if(firstByte == 0xFEu) {
            return readInt8()
        }
        return firstByte.toLong()
    }

    private fun readInt8(): Long =
        buf.readLongLE()


    fun readInt4(): Int =
        buf.readIntLE()

    fun readInt3(): Int {
        val bytes = ByteArray(3)
        buf.readBytes(bytes)
        return bytes.map { it.toInt() }.reduceRight { i, acc -> (acc shl 8) + i }
    }


    fun readInt2(): Int =
        buf.readShort().toInt()

    fun readInt1(): Int =
        buf.readByte().toInt()


    fun writeStringNullTerminated(str: String) {
        buf.writeCharSequence(str, StandardCharsets.UTF_8)
        buf.writeByte(0)
    }
    // 64 bits
    fun writeInt8(number: ULong) {
        buf.writeLongLE(number.toLong())
    }
    // 32 bits
    fun writeInt4(number: Int) {
        buf.writeIntLE(number)
    }

    fun writeInt3(number: Int) {
        for (i in 0..2) {
            buf.writeByte(((number shr i * 8) and 0xFF).toByte().toInt())
        }
    }

    // 16 bits
    fun writeInt2(number: Int) {
        buf.writeShortLE(number)
    }

    // 8 bits
    fun writeInt1(number: Int) {
        buf.writeByte(number)
    }

    fun writeIntLengthEncoded(number: ULong) {
        if(number < 251u) {
            writeInt1(number.toInt())
        } else if(number < 65536u) {
            buf.writeByte(0xFC)
            writeInt2(number.toInt())
        } else if(number < 16777216u) {
            buf.writeByte(0xFD)
            writeInt3(number.toInt())
        } else {
            buf.writeByte(0xFE)
            writeInt8(number)
        }

    }


    fun writeStringLengthEncoded(str: String) {
        writeIntLengthEncoded(str.length.toULong())
        buf.writeCharSequence(str, StandardCharsets.UTF_8)
    }

    fun writeStringEOF(str: String) {
        buf.writeCharSequence(str, StandardCharsets.UTF_8)
    }


}