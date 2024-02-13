package org.ember.records

class Case3Cell(
    private val id: Long,
    private val inOutRatio: Double = 0.0,
) : Comparable<Case3Cell> {
    /**
     * Compares this object with the specified object for order. Returns zero if this object is equal
     * to the specified [other] object, a negative number if it's less than [other], or a positive number
     * if it's greater than [other].
     */
    override fun compareTo(other: Case3Cell): Int {
        return id.compareTo(other.id)
    }

    override fun toString(): String {
        return "${id}|${String.format(".2f", inOutRatio)}"
    }
}