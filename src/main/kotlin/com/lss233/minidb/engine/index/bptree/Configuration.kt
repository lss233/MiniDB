package com.lss233.minidb.engine.index.bptree

import java.io.IOException
import java.lang.reflect.Type
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.function.BiFunction


/**
 * @param pageSize page size (in bytes)
 * @param types Keys may contain multiple columns. `types` tracks the type for each column
 * @param sizes size of each key type (in bytes)
 * @param colIDs ID of each column
 */
open class Configuration(var pageSize: Int, var types: ArrayList<Type>, var sizes: ArrayList<Int>, var colIDs: ArrayList<Int>) {

    var keySize = 0 // key size (in bytes)

    // use Integer/Float etc for primitive types

    var strColLocalId: ArrayList<Int>? = null // ID of string columns (in local Id, not the id from the whole table)

    init {
        for (each in types) {
            if (each !== Int::class.java && each !== Long::class.java && each !== Float::class.java && each !== Double::class.java && each !== String::class.java) {
                // TODO Throw Exception
                throw RuntimeException("UnknownColumnType")
            }
        }
        this.strColLocalId = ArrayList()
        for (i in types.indices) {
            if (types[i] === String::class.java) {
                this.strColLocalId!!.add(i)
            }
        }
        this.keySize = 0
        for (each in sizes) {
            this.keySize += each
        }
    }

    /*compare function with short-cut evaluation.**/
    private fun compare(
        key1: ArrayList<Any>,
        key2: ArrayList<Any>,
        func: BiFunction<Int, Int, Boolean>,
        finalValue: Boolean
    ): Boolean {
        for (j in types.indices) {
            if (types[j] === Int::class.java) {
                val ans = Integer.compare(
                    (key1[j] as Int),
                    (key2[j] as Int)
                )
                if (ans == 0) {
                    continue
                }
                return func.apply(ans, 0)
            } else if (types[j] === Long::class.java) {
                val ans = (key1[j] as Long).compareTo((key2[j] as Long))
                if (ans == 0) {
                    continue
                }
                return func.apply(ans, 0)
            } else if (types[j] === Float::class.java) {
                val ans = (key1[j] as Float).compareTo((key2[j] as Float))
                if (ans == 0) {
                    continue
                }
                return func.apply(ans, 0)
            } else if (types[j] === Double::class.java) {
                val ans = (key1[j] as Double).compareTo((key2[j] as Double))
                if (ans == 0) {
                    continue
                }
                return func.apply(ans, 0)
            } else if (types[j] === String::class.java) {
                val ans = (key1[j] as String).compareTo((key2[j] as String))
                if (ans == 0) {
                    continue
                }
                return func.apply(ans, 0)
            }
        }
        // every objects are equal
        return finalValue
    }


    // > op
    fun gt(key1: ArrayList<Any>, key2: ArrayList<Any>): Boolean {
        return compare(key1, key2, { x: Int, y: Int -> x > y }, false)
    }

    // < op
    fun lt(key1: ArrayList<Any>, key2: ArrayList<Any>): Boolean {
        return compare(key1, key2, { x: Int, y: Int -> x < y }, false)
    }

    // <= op
    fun le(key1: ArrayList<Any>, key2: ArrayList<Any>): Boolean {
        return compare(key1, key2, { x: Int, y: Int -> x < y }, true)
    }

    // != op
    fun neq(key1: ArrayList<Any>, key2: ArrayList<Any>): Boolean {
        return compare(key1, key2, { x: Int, y: Int -> x != y }, false)
    }

    // == op
    fun eq(key1: java.util.ArrayList<Any>, key2: ArrayList<Any>): Boolean {
        return !neq(key1, key2)
    }

    fun writeKey(r: ByteBuffer, key: java.util.ArrayList<Any>) {
        padKey(key)
        for (j in types.indices) {
            if (types[j] === Int::class.java) {
                r.putInt((key[j] as Int))
            } else if (types[j] === Long::class.java) {
                r.putLong((key[j] as Long))
            } else if (types[j] === Float::class.java) {
                r.putFloat((key[j] as Float))
            } else if (types[j] === Double::class.java) {
                r.putDouble((key[j] as Double))
            } else if (types[j] === String::class.java) {
                r.put((key[j] as String).toByteArray(StandardCharsets.UTF_8))
            }
        }
    }

    @Throws(IOException::class)
    fun readKey(r: ByteBuffer): ArrayList<Any> {
        val key = java.util.ArrayList(listOf(*arrayOf<Any>(types.size)))
        for (j in types.indices) {
            if (types[j] === Int::class.java) {
                key[j] = r.int
            } else if (types[j] === Long::class.java) {
                key[j] = r.long
            } else if (types[j] === Float::class.java) {
                key[j] = r.float
            } else if (types[j] === Double::class.java) {
                key[j] = r.double
            } else if (types[j] === String::class.java) {
                //TODO possible not efficient. buffer is copied into the string?
                val buffer = ByteArray(sizes[j])
                r[buffer, 0, sizes[j]]
                key[j] = String(buffer, StandardCharsets.UTF_8)
            }
        }
        return key
    }

    fun printKey(key: ArrayList<Any>) {
        println(keyToString(key))
    }

    fun keyToString(key: ArrayList<Any>): String {
        val ans = StringBuilder()
        ans.append("[")
        for (i in types.indices) {
            if (types[i] === Int::class.java) {
                ans.append(key[i] as Int?)
                ans.append(' ')
            } else if (types[i] === Long::class.java) {
                ans.append(key[i] as Long?)
                ans.append(' ')
            } else if (types[i] === Float::class.java) {
                ans.append(key[i] as Float?)
                ans.append(' ')
            } else if (types[i] === Double::class.java) {
                ans.append(key[i] as Double?)
                ans.append(' ')
            } else if (types[i] === String::class.java) {
                ans.append(key[i] as String)
                ans.append(' ')
            }
        }
        ans.append("]")
        return ans.toString()
    }

    fun padString(arg: String, nBytes: Int): String {
        val size = arg.toByteArray(StandardCharsets.UTF_8).size
        if (size > nBytes) {
            throw RuntimeException("StringLengthOverflow")
        }
        return if (size == nBytes) {
            arg
        } else arg + String(CharArray(nBytes - size)).replace('\u0000', ' ')
    }

    fun padKey(key: java.util.ArrayList<Any>): ArrayList<Any> {
        for (i in strColLocalId!!) {
            key[i] = padString(key[i] as String, sizes[i])
        }
        return key
    }
}