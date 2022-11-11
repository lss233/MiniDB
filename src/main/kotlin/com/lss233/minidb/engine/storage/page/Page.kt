package com.lss233.minidb.engine.storage.page

/**
 * 页表
 * 每张页存16KB的数据
 */
class Page {

    /**
     * 页号
     */
    var pageId:Int ?= null

    /**
     * 记录总数
     */
    var recordNum:Int ?= null

    /**
     * 最小的ID号
     */
    var minId:Int ?= null

    /**
     * 最大的ID号
     */
    var maxId:Int ?= null

    /**
     * 已用的空间
     */
    var usedSpace:Int ?= null

    /**
     * 文件中开始的byte位
     */
    var beginByte:Int ?= null

    /**
     * 文件中结束的byte位
     */
    var endByte:Int ?= null

    /**
     * 当前页存储的数据
     */
    var data:ByteArray ?= null

}