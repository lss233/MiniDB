package com.lss233.minidb.engine.schema

class Column(val name: String) {

    override fun equals(other: Any?): Boolean {
        return when (other) {
            !is Column -> false
            else -> (this === other) || (this.name == other.name)
        }
    }

    override fun hashCode(): Int {
        return name.hashCode()
    }
}
