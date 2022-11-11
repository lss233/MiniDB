package com.lss233.minidb.engine.storage.page

/**
 * 段表
 * 每个段表可以存500个页表
 */
class Segment {

    /**
     * 段表号
     */
    var segmentId = 0

    /**
     * 页数
     */
    var pageNum = 0

    /**
     * 已用空间
     */
    var usedSpace = 0

    /**
     * 最小页号数
     */
    var minId = 0

    /**
     * 最大页号数
     */
    var maxId = 0

    /**
     * 当前段内的页数据
     */
    var pages: ArrayList<Page>? = null
}