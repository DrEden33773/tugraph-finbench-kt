@file:Suppress("UnstableApiUsage")

package org.ember

import com.antgroup.geaflow.api.context.RuntimeContext
import com.antgroup.geaflow.api.function.RichFunction
import com.antgroup.geaflow.api.function.io.SourceFunction
import com.antgroup.geaflow.api.window.IWindow
import com.antgroup.geaflow.example.function.FileSource.FileLineParser
import com.google.common.io.Resources
import okio.IOException
import java.nio.charset.Charset

class DataSource<OUT>(
    private val filePaths: List<String>,
    private val parsers: List<FileLineParser<OUT>>
) : RichFunction(), SourceFunction<OUT> {
    @Transient
    var runtimeCtx: RuntimeContext? = null
    private var readPos: Int = -1
    private var records: List<OUT> = listOf()

    private fun readFileLines(filepath: String, parser: FileLineParser<OUT>): List<OUT> {
        try {
            val lines = Resources.readLines(Resources.getResource(filepath), Charset.defaultCharset())
            val res = mutableListOf<OUT>()
            var isHeader = true
            for (line in lines) {
                if (isHeader) {
                    isHeader = false
                } else {
                    res += parser.parse(line)
                }
            }
            return res
        } catch (e: IOException) {
            throw RuntimeException("Error occurred while reading file: $filepath", e)
        }
    }

    override fun open(runtimeContext: RuntimeContext?) {
        this.runtimeCtx = runtimeContext
    }

    override fun init(parallel: Int, index: Int) {
        /* read */
        assert(filePaths.size == parsers.size)
        for ((filepath, parser) in filePaths.zip(parsers)) {
            this.records += readFileLines(filepath, parser)
        }
        /* parallel */
        if (parallel > 1) {
            val allRecords = this.records
            this.records = listOf()
            for (i in index until allRecords.size step parallel) {
                this.records += allRecords[i]
            }
        }
    }

    override fun fetch(window: IWindow<OUT>?, ctx: SourceFunction.SourceContext<OUT>?): Boolean {
        if (readPos < 0) {
            readPos = 0
        }

        while (readPos < records.size) {
            val out = records[readPos]
            val windowId = window?.assignWindow(out)
            if (window?.windowId() == windowId) {
                ctx?.collect(out)
                readPos += 1
            } else {
                break
            }
        }

        return readPos < records.size
    }

    override fun close() {
    }
}