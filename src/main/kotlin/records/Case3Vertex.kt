package org.ember.records

data class Case3Vertex(
    val accountId: Long,
) {
    var inSum = 0.0
    var outSum = 0.0
    var inOutRatio = 0.0
    var hasIn = false
    var hasOut = false

    fun toCase3Cell(): Case3Cell {
        return Case3Cell(accountId, inOutRatio)
    }
}
