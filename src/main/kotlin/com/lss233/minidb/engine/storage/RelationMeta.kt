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
 * Relational table metadata
 *
 * which records some key information in the current relational table, similar to a table header.
 *
 * Examples include the number of columns, column types, size, number of records, and so on
 */
class RelationMeta : Serializable {

    var nextRowID: Long = 0 // the next available row ID

    var ncols = 0 // number of columns

    var colnames = ArrayList<String>() // column names

    var coltypes = ArrayList<Type>() // column types

    var colsizes = ArrayList<Int>() // column sizes. (esp. for variable length string)

    var nullableColIds = ArrayList<Int>() // nullable columns. other columns are non-nullable

    // super key is just the abstraction of uniqueness (on one or multiple columns).
    // colIDs for super keys (primary key is just a normal super key)
    var superKeys = ArrayList<ArrayList<Int>>()

    // index is just the abstraction of non-uniqueness.
    // colIDs for indices
    var indices = ArrayList<ArrayList<Int>>()

    /**
     * validate the meta configuration
     */
    @Throws(MiniDBException::class)
    fun validate(): Boolean {
        if (ncols <= 0) return false
        if (ncols != colnames.size) return false
        if (ncols != coltypes.size) return false
        if (ncols != colsizes.size) return false
        // check unsupported types
        for (each in coltypes) {
            if (each != java.lang.Integer::class.java && each != java.lang.Double::class.java
                && each != java.lang.String::class.java && each != java.lang.Float::class.java
                && each != java.lang.Long::class.java && each != java.lang.Boolean::class.java
                && each != java.lang.Short::class.java) {
                throw MiniDBException(java.lang.String.format(MiniDBException.UnknownColumnType, each))
            }
        }
        // check malformed sizes
        for (i in 0 until ncols) {
            if (coltypes[i] == java.lang.Integer::class.java) {
                if (colsizes[i] != 4) return false
            } else if (coltypes[i] == java.lang.Long::class.java) {
                if (colsizes[i] != 8) return false
            } else if (coltypes[i] == java.lang.Float::class.java) {
                if (colsizes[i] != 4) return false
            } else if (coltypes[i] == java.lang.Double::class.java) {
                if (colsizes[i] != 8) return false
            } else if (coltypes[i] == java.lang.String::class.java) {
                if (colsizes[i] <= 0) return false
            } else if (coltypes[i] == java.lang.Boolean::class.java) {
                if (colsizes[i] != 4) return false
            }
        }
        for (each in nullableColIds) {
            if (each !in 0 until ncols) return false
        }
        for (each in superKeys) {
            for (eachEach in each) {
                if (eachEach !in 0 until ncols) return false
            }
        }
        for (each in indices) {
            for (eachEach in each) {
                if (eachEach !in 0 until ncols) return false
            }
        }

        // columns concerned with candidate keys and indices are not nullable
        val nonNullableCols = HashSet<Int>()
        for (each in superKeys) {
            nonNullableCols.addAll(each)
        }
        for (each in indices) {
            nonNullableCols.addAll(each)
        }
        val nullableCols = HashSet(nullableColIds)
        // non-nullable constraint is implicit
        nullableCols.removeAll(nonNullableCols)
        nullableColIds = ArrayList(nullableCols)
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
