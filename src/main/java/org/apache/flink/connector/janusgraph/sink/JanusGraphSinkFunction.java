package org.apache.flink.connector.janusgraph.sink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.janusgraph.internal.JanusGraphOutputFormat;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;

/** Sink function. */
public class JanusGraphSinkFunction<T> extends RichSinkFunction<T>
        implements CheckpointedFunction, InputTypeConfigurable {

    private final JanusGraphOutputFormat<T> outputFormat;

    public JanusGraphSinkFunction(JanusGraphOutputFormat<T> outputFormat) {
        this.outputFormat = Preconditions.checkNotNull(outputFormat);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext runtimeContext = getRuntimeContext();
        outputFormat.setRuntimeContext(runtimeContext);
        outputFormat.open(
                runtimeContext.getIndexOfThisSubtask(),
                runtimeContext.getNumberOfParallelSubtasks());
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        outputFormat.writeRecord(value);
    }

    @Override
    public void close() throws Exception {
        super.close();
        outputFormat.close();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {}

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        outputFormat.flush();
    }

    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        outputFormat.setInputType(type, executionConfig);
    }
}
