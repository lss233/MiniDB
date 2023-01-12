package com.lss233.minidb.exception

class MiniDBException(m: String) : RuntimeException(m) {
    companion object {
        var StringLengthOverflow = "String Length Exceeds the limits! The limit is %d (bytes) " +
                "and the string (%s) has a length of %d."
        var DuplicateValue = "Duplicate value (%s) for the same key (%s)!"
        var UnknownColumnType = "Unknown column type (%s). " +
                "Only Integer, Long, Float, Double and String are supported!"
        var InvalidBPTreeState = "Internal error! Invalid B+ tree state."
        var BadNodeType = "Internal error! Bad node type."
    }
}
