package org.ember.executor

import com.antgroup.geaflow.api.window.impl.AllWindow
import com.antgroup.geaflow.env.Environment
import com.antgroup.geaflow.example.function.FileSink
import com.antgroup.geaflow.example.function.FileSource.FileLineParser
import com.antgroup.geaflow.example.util.EnvironmentUtil
import com.antgroup.geaflow.example.util.PipelineResultCollect
import com.antgroup.geaflow.example.util.ResultValidator
import com.antgroup.geaflow.model.graph.edge.IEdge
import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge
import com.antgroup.geaflow.model.graph.vertex.IVertex
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex
import com.antgroup.geaflow.pipeline.IPipelineResult
import com.antgroup.geaflow.pipeline.PipelineFactory
import com.antgroup.geaflow.view.GraphViewBuilder
import com.antgroup.geaflow.view.IViewDesc
import org.ember.DataSource
import org.ember.Env
import org.ember.OrderedFileSink
import org.ember.algorithms.Case3Algorithm
import org.ember.records.Case3Cell
import org.ember.records.Case3Vertex
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class Case3 {
    companion object {
        const val RESULT_PATH = "./result/case3"
        val LOGGER: Logger = LoggerFactory.getLogger(Case3::class.java)
    }

    private val vertexFilenames = listOf("Account.csv")
    private val edgeFilenames = listOf("AccountTransferAccount.csv")
    private val vertexParsers = listOf(FileLineParser {
        val fields = it.split("\\|")
        val accountId = fields[0].toLong()
        val vertex: IVertex<Long, Case3Vertex> = ValueVertex(accountId, Case3Vertex(accountId))
        listOf(vertex)
    })
    private val edgeParsers = listOf(FileLineParser {
        val fields = it.split("\\|")
        val srcId = fields[0].toLong()
        val dstId = fields[1].toLong()
        val value = fields[2].toDouble()
        val edge: IEdge<Long, Double> = ValueEdge(srcId, dstId, value)
        listOf(edge)
    })

    private fun submit(env: Environment): IPipelineResult<Any> {
        val pipeline = PipelineFactory.buildPipeline(env)
        val envConfig = env.environmentContext.config
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_PATH)
        ResultValidator.cleanResult(RESULT_PATH)

        pipeline.submit {
            val vertices = it.buildSource(
                DataSource(vertexFilenames, vertexParsers), AllWindow.getInstance()
            ).withParallelism(Env.parallelismMax())
            val edges = it.buildSource(
                DataSource(edgeFilenames, edgeParsers), AllWindow.getInstance()
            ).withParallelism(Env.parallelismMax())

            val graphViewDesc = GraphViewBuilder.createGraphView("Case3")
                .withShardNum(Env.parallelismMax())
                .withBackend(IViewDesc.BackendType.Memory)
                .build()
            val graphWindow = it.buildWindowStreamGraph(
                vertices, edges, graphViewDesc
            )
            val sinkFunc = OrderedFileSink<Case3Cell>()

            graphWindow
                .compute(Case3Algorithm())
                .compute(Env.parallelismMax())
                .vertices
                .filter { v -> v.value.hasIn && v.value.hasOut }
                .map { v -> v.value.toCase3Cell() }
                .sink(sinkFunc)
                .withParallelism(Env.SINK_PARALLELISM_MAX)
        }

        return pipeline.execute()
    }

    fun run(args: Array<String>) {
        LOGGER.info("*** Start Case3 ***")
        val env = EnvironmentUtil.loadEnvironment(args)
        val res = submit(env)
        PipelineResultCollect.get(res)
        env.shutdown()
        LOGGER.info("*** End Case3 ***")
    }
}