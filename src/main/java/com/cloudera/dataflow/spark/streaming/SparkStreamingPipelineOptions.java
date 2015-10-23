package com.cloudera.dataflow.spark.streaming;

import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;

import com.cloudera.dataflow.spark.SparkPipelineOptions;

/**
 * Options used to configure Spark streaming
 */
public interface SparkStreamingPipelineOptions extends SparkPipelineOptions {

  @Override
  @Default.String("local[*]")
  String getSparkMaster();

  @Description("The time interval (in msec) at which streaming data will be divided into batches")
  @Default.Long(1000)
  Long getBatchInterval();

  void setBatchInterval(Long batchInterval);

  @Description("Timeout to wait (in msec) for the streaming execution so stop, -1 runs until execution is stopped")
  @Default.Long(-1)
  Long getTimeout();

  void setTimeout(Long batchInterval);

  @Override
  @Default.Boolean(true)
  boolean isStreaming();

  @Override
  @Default.String("spark streaming dataflow pipeline job")
  String getAppName();
}
