package com.lss233.minidb.engine.storage.struct

/**
 * 定义查询范围
 */
class SelectRange {
    class Range {
        var begin = 0
        var end = 0
    }

    var ranges: List<Range> = ArrayList()
    var size = 0
}
