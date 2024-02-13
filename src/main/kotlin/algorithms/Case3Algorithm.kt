package org.ember.algorithms

import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction
import com.antgroup.geaflow.example.function.AbstractVcFunc
import org.ember.records.Case3Vertex

class Case3Algorithm : VertexCentricCompute<Long, Case3Vertex, Double, Double>(2L) {
    /**
     * Returns vertex centric combine function.
     */
    override fun getCombineFunction(): VertexCentricCombineFunction<Double>? {
        return null
    }

    override fun getComputeFunction(): VertexCentricComputeFunction<Long, Case3Vertex, Double, Double> {
        TODO("Not yet implemented")
    }
}

class Case3ComputeFunction : AbstractVcFunc<Long, Case3Vertex, Double, Double>() {
    /**
     * Perform traversing based on message iterator during iterations.
     */
    override fun compute(vertexId: Long?, messageIterator: MutableIterator<Double>?) {
        val currVertex = this.context.vertex().get().value
        var inSum = currVertex.inSum
        var outSum = currVertex.outSum


        if (this.context.iterationId == 1L) {
            val edges = this.context.edges().outEdges
            if (edges.isNotEmpty()) {
                currVertex.hasOut = true
            }
            edges.forEach {
                this.context.sendMessage(it.targetId, it.value)
                outSum += it.value
                currVertex.outSum = outSum
            }
            return
        }

        if (messageIterator?.hasNext() == true) {
            currVertex.hasIn = true
        }
        while (messageIterator?.hasNext() == true) {
            inSum += messageIterator.next()
        }
        currVertex.inSum = inSum

        if (inSum == 0.0 || outSum == 0.0) {
            return
        }

        val inOutRatio = inSum / outSum
        currVertex.inOutRatio = inOutRatio
    }

}