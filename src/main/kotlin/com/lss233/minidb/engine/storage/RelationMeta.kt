package com.lss233.minidb.engine.storage

import com.lss233.minidb.exception.MiniDBException
import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.Serializable
import java.lang.reflect.Type
import java.nio.file.Files
import java.nio.file.Paths

/**
 *
 */
class RelationMeta : Serializable{

    var nextRowID: Long = 0 // the next available row ID

    var ncols = 0 // number of columns

    var colnames: ArrayList<String>? = null // column names

    var coltypes: ArrayList<Type>? = null // column types

    var colsizes: ArrayList<Int>? = null // column sizes. (esp. for variable length string)

    var nullableColIds: ArrayList<Int>? = null // nullable columns. other columns are non-nullable

    // super key is just the abstraction of uniqueness (on one or multiple columns).
    // colIDs for super keys (primary key is just a normal super key)
    var superKeys: ArrayList<ArrayList<Int>>? = null

    // index is just the abstraction of non-uniqueness.
    // colIDs for indices
    var indices: ArrayList<ArrayList<Int>>? = null


    /**
     * validate the meta configuration
     */
    @Throws(MiniDBException::class)
    fun validate(): Boolean {
        if (colnames == null) return false
        if (coltypes == null) return false
        if (colsizes == null) return false
        if (nullableColIds == null) return false
        if (superKeys == null) return false
        if (indices == null) return false
        if (ncols <= 0) return false
        if (ncols != colnames!!.size) return false
        if (ncols != coltypes!!.size) return false
        if (ncols != colsizes!!.size) return false
        // check unsupported types
        for (each in coltypes!!) {
            if (each != java.lang.Integer::class.java && each != java.lang.Double::class.java
                    && each != java.lang.String::class.java && each != java.lang.Float::class.java
                    && each != java.lang.Double::class.java) {
                throw MiniDBException(java.lang.String.format(MiniDBException.UnknownColumnType, each))
            }
        }
        // check malformed sizes
        for (i in 0 until ncols) {
            if (coltypes!![i] == java.lang.Integer::class.java) {
                if (colsizes!![i] != 4) return false
            } else if (coltypes!![i] == java.lang.Long::class.java) {
                if (colsizes!![i] != 8) return false
            } else if (coltypes!![i] == java.lang.Float::class.java) {
                if (colsizes!![i] != 4) return false
            } else if (coltypes!![i] == java.lang.Double::class.java) {
                if (colsizes!![i] != 8) return false
            } else if (coltypes!![i] == java.lang.String::class.java) {
                if (colsizes!![i] <= 0) return false
            }
        }
        for (each in nullableColIds!!) {
            if (each !in 0 until ncols) return false
        }
        for (each in superKeys!!) {
            for (eachEach in each) {
                if (eachEach !in 0 until ncols) return false
            }
        }
        for (each in indices!!) {
            for (eachEach in each) {
                if (eachEach !in 0 until ncols) return false
            }
        }

        // columns concerned with candidate keys and indices are not nullable
        val nonNullableCols = HashSet<Int>()
        for (each in superKeys!!) {
            nonNullableCols.addAll(each)
        }
        for (each in indices!!) {
            nonNullableCols.addAll(each)
        }
        val nullableCols = HashSet(nullableColIds!!)
        // non-nullable constraint is implicit
        nullableCols.removeAll(nonNullableCols)
        nullableColIds = java.util.ArrayList(nullableCols)
        return true
    }

    @Throws(IOException::class, ClassNotFoundException::class)
    fun read(filepath: String): RelationMeta {
        val ois = ObjectInputStream(Files.newInputStream(Paths.get(filepath)))
        val tmp = ois.readObject() as RelationMeta
        ois.close()
        return tmp
    }

    @Throws(IOException::class)
    fun write(filepath: String) {
        val oos = ObjectOutputStream(Files.newOutputStream(Paths.get(filepath)))
        oos.writeObject(this)
        oos.close()
    }

}