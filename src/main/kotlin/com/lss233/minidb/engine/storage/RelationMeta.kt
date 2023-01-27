package com.lss233.minidb.engine.storage

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