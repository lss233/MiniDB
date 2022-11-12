package com.lss233.minidb.engine.storage.result

import com.lss233.minidb.engine.storage.page.Page

class DBResult {

    var code = 0
    var type: DBResultType? = null
    var queryResult: String? = null
    var transactionId = 0
    var data: ByteArray? = null
    var page: List<Page>? = null
    var affectedNum = 0

    constructor() {}

    constructor(code: Int, type: DBResultType?, queryResult: String?, transcationId: Int)  {
        this.code = code
        this.type = type
        this.queryResult = queryResult
        transactionId = transcationId
    }


    fun error(code: Int) {
        this.code = code
    }

    fun error(code: Int, queryResult: String?) {
        this.code = code
        this.queryResult = queryResult
    }

    fun buildSelect(transactionId: Int, pages: List<Page>?): DBResult {
        val result = DBResult()
        result.code = 200
        result.transactionId = transactionId
        result.type = DBResultType.SELECT
        if (pages != null && pages.size != 0) {
            result.queryResult = "query ok!"
            result.page = pages
        } else {
            result.queryResult = "query ok!"
            result.code = 201 //代表数据是空的
            result.data = ByteArray(0)
        }
        return result
    }


    fun buildInsert(transactionId: Int): DBResult {
        val result = DBResult()
        result.code = 200
        result.transactionId = transactionId
        result.type = DBResultType.INSERT
        result.queryResult = "query ok!"
        return result
    }

    fun buildDelete(transactionId: Int): DBResult {
        val result = DBResult()
        result.code = 200
        result.transactionId = transactionId
        result.type = DBResultType.DELETE
        result.queryResult = "query ok!"
        return result
    }

    fun buildUpdate(transactionId: Int): DBResult {
        val result = DBResult()
        result.code = 200
        result.transactionId = transactionId
        result.type = DBResultType.UPDATE
        result.queryResult = "query ok!"
        return result
    }

    fun buildEmpty(): DBResult {
        val result = DBResult()
        result.code = 404
        return result
    }

}