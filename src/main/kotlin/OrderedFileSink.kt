package org.ember

import com.antgroup.geaflow.api.context.RuntimeContext
import com.antgroup.geaflow.api.function.RichFunction
import com.antgroup.geaflow.api.function.io.SinkFunction
import com.antgroup.geaflow.common.config.ConfigKey
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException
import com.antgroup.geaflow.example.function.FileSink.FILE_OUTPUT_APPEND_ENABLE
import com.antgroup.geaflow.example.function.FileSink.OUTPUT_DIR
import org.apache.commons.io.FileUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.nio.charset.Charset
import java.util.*

class OrderedFileSink<OUT : Comparable<OUT>> : RichFunction(), SinkFunction<OUT> {
    companion object {
        const val HEADER = "id|value"
        val LOGGER: Logger = LoggerFactory.getLogger(OrderedFileSink::class.java)
    }

    private var file: File? = null

    private val queue = PriorityQueue<OUT>()

    /**
     * Open function.
     */
    override fun open(runtimeContext: RuntimeContext?) {
        if (runtimeContext == null) return

        val prefix = runtimeContext.configuration.getString(OUTPUT_DIR)
        val nth = runtimeContext.taskArgs.taskIndex
        val filename = "${prefix}/result_${nth}"
        LOGGER.info("Sink file name: $filename")

        val shouldAppend = runtimeContext.configuration.getBoolean(
            ConfigKey(FILE_OUTPUT_APPEND_ENABLE, true)
        )
        file = File(filename)

        try {
            if (!shouldAppend && file!!.exists()) {
                try {
                    FileUtils.forceDelete(file)
                } catch (e: Exception) {
                    // ignore
                }
            }
            if (!file!!.exists()) {
                if (!file!!.parentFile.exists()) {
                    file!!.parentFile.mkdirs()
                }
                file!!.createNewFile()
            }
        } catch (e: IOException) {
            throw GeaflowRuntimeException(e)
        }
    }

    /**
     * Close function.
     */
    override fun close() {
        // Now, write data in `queue` to file
        file?.let {
            try {
                FileUtils.write(file, HEADER + "\n", Charset.defaultCharset(), true)
                while (queue.isNotEmpty()) {
                    FileUtils.write(file, queue.poll().toString() + "\n", Charset.defaultCharset(), true)
                }
            } catch (e: IOException) {
                throw GeaflowRuntimeException(e)
            }
        }
    }

    /**
     * The write method for Outputting data t.
     */
    override fun write(out: OUT) {
        // simply write data into `queue`
        queue.add(out)
    }
}