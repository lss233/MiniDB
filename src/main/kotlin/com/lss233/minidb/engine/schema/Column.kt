package com.lss233.minidb.engine.schema

/**
 * 进行关系运算过滤中的封装列的信息实体
 * 作为参数使用
 */

class Column(val name: String) {

    /**
     * 重写equals方法
     * 比较列仅需要名字相同既可以
     */
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
