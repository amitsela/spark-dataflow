package com.cloudera.dataflow.spark.streaming;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsRegistrar;
import com.google.common.collect.ImmutableList;

public class SparkStreamingPipelineOptionsRegistrar implements PipelineOptionsRegistrar {

  @Override
  public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
    return ImmutableList.<Class<? extends PipelineOptions>>of(SparkStreamingPipelineOptions.class);
  }
}
