package com.lss233.minidb.utils

/**
 * byte操作的util
 */
class ByteUtil {

    companion object {
        @JvmStatic
        fun arraycopy(target: ByteArray, pos: Int, newBytes: ByteArray): ByteArray {
            System.arraycopy(newBytes, 0, target, pos, newBytes.size)
            return target
        }

        @JvmStatic
        fun intToByte4(sum: Int): ByteArray {
            val arr = ByteArray(4)
            arr[0] = (sum shr 24).toByte()
            arr[1] = (sum shr 16).toByte()
            arr[2] = (sum shr 8).toByte()
            arr[3] = (sum and 0xff).toByte()
            return arr
        }

        @JvmStatic
        fun byteToInt4(bytes: ByteArray): Int {
            return bytes[3].toInt() and 0xFF or (
                    bytes[2].toInt() and 0xFF shl 8) or (
                    bytes[1].toInt() and 0xFF shl 16) or (
                    bytes[0].toInt() and 0xFF shl 24)
        }

        @JvmStatic
        fun getTimeStampByte4(): ByteArray {
            val time = System.currentTimeMillis().toInt()
            return intToByte4(time)
        }
    }
}