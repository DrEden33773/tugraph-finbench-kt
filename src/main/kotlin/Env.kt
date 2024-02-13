package org.ember

class Env {
    companion object {
        const val SINK_PARALLELISM_MAX = 1
        private const val PARALLEL = true
        fun parallelismMax(): Int {
            return if (PARALLEL) {
                Runtime.getRuntime().availableProcessors().takeHighestOneBit()
            } else {
                1
            }
        }
    }
}