package com.lss233.minidb.engine

/**
 * 一个表格实体，封装了所有的列，并且带有列式存储结构转行式存储结构的方法
 * @param tableName 表名
 * @param attributes 所有的列数据
 * @author  <a href="mailto:icebigpig404@foxmail.com">icebigpig</a>
 * @date    2022/10/21 11:10
 * @version 1.0
 */

class NTable constructor(private var tableName:String, private var attributes:List<Attribute<Any>>) {

    fun getTableName():String {
        return this.tableName
    }

    fun setTableName(tableName: String) {
        this.tableName = tableName
    }

    /**
     * 设置整个表的数据,即所有列的数据
     * @param attributes 所有的列的数据
     */
    fun setAttributes(attributes: List<Attribute<Any>>) {
        this.attributes = attributes
    }

    /**
     * 通过列名获取一列行式存储的数据
     * @param colName 列名
     * @return 得到一个Attribute实体，一整列的数据
     */
    fun getAttribute(colName:String): Attribute<Any>? {
        for(item in attributes) {
            if (item.getColName() == colName) {
                return item
            }
        }
        // TODO 这里的return null 是否需要适当处理？
        return null
    }

    /**
     * 获取当前表格指定行范围的数据(列式存储转行式存储)
     * @param startLine 开始行
     * @param endLine 结束行
     * @return 行式存储结构数据
     */
    fun getNTuplesByLineNumber(startLine:Int, endLine:Int) : NTuple{
        val nTuple = NTuple()
        // TODO 处理多行
        return nTuple
    }

    /**
     * 获取（行式存储结构）一整行的数据
     * @param line 行号（从0开始）
     * @return 一整行的数据
     */
    fun getNTuple(line:Int): NTuple {
        val nTuple = NTuple()
        for (i in this.attributes) {
            nTuple.plus(Cell(i.getColName(), i[line]))
        }
        return nTuple
    }
}