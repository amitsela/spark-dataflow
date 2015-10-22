package com.cloudera.dataflow.spark.streaming;

import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;

public final class SparkStreamingPipelineOptionsFactory {

  private SparkStreamingPipelineOptionsFactory() {
  }

  public static SparkStreamingPipelineOptions create() {
    return PipelineOptionsFactory.as(SparkStreamingPipelineOptions.class);
  }
}
