/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.dataflow.spark.streaming;

import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;

import org.joda.time.Duration;

/**
 * Spark streaming is a micro-batch stream processing framework, so Spark's "batchInterval" is a
 * FixedWindow strategy. Dataflow requires window operations on unbounded collections when using
 * group by key or combine, but Spark streaming supports the operations on the micro batches which
 * are effectively a FixedWindow strategy
 */
public final class SparkStreamingWindowStrategy {

  private SparkStreamingWindowStrategy() {
  }

  public static WindowingStrategy<?, ?> of(Long batchInterval) {
    return WindowingStrategy.of(FixedWindows.of(
            Duration.millis(batchInterval)));
  }
}
