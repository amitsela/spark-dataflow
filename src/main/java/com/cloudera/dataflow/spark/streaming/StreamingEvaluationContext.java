package com.cloudera.dataflow.spark.streaming;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.common.collect.Iterables;

import com.cloudera.dataflow.spark.EvaluationContext;
import com.cloudera.dataflow.spark.TransformTranslator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Streaming evaluation context helps to handle streaming
 */
public class StreamingEvaluationContext extends EvaluationContext {

  private final JavaStreamingContext jssc;
  private final long timeout;
  private final Map<PValue, DStreamHolder<?>> pstreams = new LinkedHashMap<>();
  private final Set<DStreamHolder<?>> leafStreams = new LinkedHashSet<>();

  public StreamingEvaluationContext(JavaSparkContext jsc,
                                    Pipeline pipeline,
                                    JavaStreamingContext jssc, long timeout) {
    super(jsc, pipeline);
    this.jssc = jssc;
    this.timeout = timeout;
  }

  /**
   * Because stream processing is a continuous, recurring processing of micro batches - we need to provide a "snapshot" of the evaluation
   * context with the appropriate "current transform"
   */
  public StreamingEvaluationContext recurringEvaluationContext() {
    StreamingEvaluationContext context = new StreamingEvaluationContext(jssc.sparkContext(),
                                                                        getPipeline(), jssc, timeout);
    context.setCurrentTransform(this.currentTransform);
    return context;
  }

  /**
   * DStream holder
   * Can also crate a DStream from a supplied queue of values, but mainly for testing
   */
  private class DStreamHolder<T> {

    private Iterable<Iterable<T>> values;
    private Coder<T> coder;
    private JavaDStream<T> dStream;

    public DStreamHolder(Iterable<Iterable<T>> values, Coder<T> coder) {
      this.values = values;
      this.coder = coder;
    }

    public DStreamHolder(JavaDStream<T> dStream) {
      this.dStream = dStream;
    }

    @SuppressWarnings("unchecked")
    public JavaDStream<T> getDStream() {
      if (dStream == null) {
        // create the DStream from values
        Queue<JavaRDD<T>> rddQueue = new LinkedBlockingQueue<>();
        for (Iterable<T> v : values) {
          setOutputRDDFromValues(currentTransform.getTransform(), v, coder);
          rddQueue.offer((JavaRDD<T>) getOutputRDD(currentTransform.getTransform()));
        }
        dStream = jssc.queueStream(rddQueue, true);
      }
      return dStream;
    }
  }

  public <T> void setDStreamFromQueue(PTransform<?, ?> transform, Iterable<Iterable<T>> values,
                                      Coder<T> coder) {
    pstreams.put((PValue) getOutput(transform), new DStreamHolder<>(values, coder));
  }

  public <T, R extends JavaRDDLike<T, R>> void setStream(PTransform<?, ?> transform,
                                                         JavaDStreamLike<T, ?, R> dStream) {
    PValue pvalue = (PValue) getOutput(transform);
    @SuppressWarnings("unchecked")
    DStreamHolder<T> dStreamHolder = new DStreamHolder((JavaDStream) dStream);
    pstreams.put(pvalue, dStreamHolder);
    leafStreams.add(dStreamHolder);
  }

  public JavaDStreamLike<?, ?, ?> getStream(PTransform<?, ?> transform) {
    PValue pvalue = (PValue) getInput(transform);
    DStreamHolder<?> dStreamHolder = pstreams.get(pvalue);
    JavaDStreamLike<?, ?, ?> dStream = dStreamHolder.getDStream();
    leafStreams.remove(dStreamHolder);
    return dStream;
  }

  // used to set the RDD from the DStream in the RDDHolder for transformation
  public <T> void setInputRDD(PTransform<? extends PInput, ?> transform, JavaRDDLike<T, ?> rdd) {
    setRDD((PValue) getInput(transform), rdd);
  }

  // used to get the RDD transformation output and use it as the DStream transformation output
  public JavaRDDLike<?, ?> getOutputRDD(PTransform<?, ?> transform) {
    return getRDD((PValue) getOutput(transform));
  }

  public JavaStreamingContext getStreamingContext() {
    return jssc;
  }

  @Override
  protected void computeOutputs() {
    for (DStreamHolder<?> streamHolder : leafStreams) {
      @SuppressWarnings("unchecked")
      JavaDStream<Object> stream = (JavaDStream<Object>) streamHolder.getDStream();
      //TODO - cache/persist ?
      stream.foreachRDD(new Function<JavaRDD<Object>, Void>() {
        @Override
        public Void call(JavaRDD<Object> rdd) throws Exception {
          rdd.count();
          return null;
        }
      }); // force a DStream action
    }
  }

  @Override
  public void close() {
    if (timeout > 0) {
      jssc.awaitTerminationOrTimeout(timeout);
    } else {
      jssc.awaitTermination();
    }
    //TODO - stop gracefully ?
    jssc.stop(false, false);
    super.close();
  }
}
