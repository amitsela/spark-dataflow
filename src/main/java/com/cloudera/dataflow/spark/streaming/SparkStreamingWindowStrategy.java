package com.cloudera.dataflow.spark.streaming;

import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;

import org.joda.time.Duration;

/**
 * Spark streaming is a micro-batch stream processing framework, so Spark's "batchInterval" is a
 * FixedWindow strategy
 * Dataflow requires window operations on unbounded collections when using
 * group by key or combine, but Spark streaming supports the operations on the micro batches which
 * are effectively a FixedWindow strategy
 */
public class SparkStreamingWindowStrategy {

  public static WindowingStrategy<?, ?> getWindowStrategy(Long batchInterval) {
    return WindowingStrategy.of(FixedWindows.of(
        Duration.millis(batchInterval)));
  }
}
