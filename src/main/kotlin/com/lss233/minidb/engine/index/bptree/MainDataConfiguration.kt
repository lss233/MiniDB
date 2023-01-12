package com.lss233.minidb.engine.index.bptree

import java.lang.reflect.Type

class MainDataConfiguration(types: ArrayList<Type>, sizes: ArrayList<Int>, colIDs: ArrayList<Int>):
    Configuration(0, types, sizes, colIDs) {

    var nValidPointerInFreePage: Int

    init {
        // pad keysize to multiples of 16
        val BASE = 16
        var tmpSize = keySize
        tmpSize += 8 // space for `RowID`
        if (tmpSize % BASE != 0) {
            tmpSize += BASE - tmpSize % BASE
        }
        pageSize = Math.max(32, tmpSize)
        nValidPointerInFreePage = pageSize / 8 - 1
    }
}
