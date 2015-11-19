/*
 * Copyright (c) 2015 Amit Sela
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.dataflow.io;

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.common.base.Preconditions;

import com.cloudera.dataflow.spark.streaming.SparkStreamingWindowStrategy;

/**
 * Create an input stream from Queue
 * @param <T> stream type
 */
public final class CreateStream<T> {

  private CreateStream() {
  }

  /**
   * Define the input stream to create from queue
   *
   * @param queuedValues defines the input stream
   * @param batchInterval Spark streaming batch interval
   * @param <T> stream type
   * @return the queue that defines the input stream
   */
  public static <T> QueuedValues<T> fromQueue(Iterable<Iterable<T>> queuedValues, Long batchInterval) {
    return new QueuedValues<>(queuedValues, batchInterval);
  }

  public static final class QueuedValues<T> extends PTransform<PInput, PCollection<T>> {

    private final Iterable<Iterable<T>> queuedValues;
    private final Long batchInterval;

    QueuedValues(Iterable<Iterable<T>> queuedValues, Long batchInterval) {
      Preconditions.checkNotNull(queuedValues,
                                 "need to set the queuedValues of an Create.QueuedValues transform");
      Preconditions.checkNotNull(batchInterval,
                                 "need to set the batchInterval of a SocketIO.Read transform");
      this.queuedValues = queuedValues;
      this.batchInterval = batchInterval;
    }

    public Iterable<Iterable<T>> getQueuedValues() {
      return queuedValues;
    }

    @Override
    public PCollection<T> apply(PInput input) {
      // Spark streaming micro batches are bounded by default
      return PCollection.createPrimitiveOutputInternal(input.getPipeline(),
                                                       SparkStreamingWindowStrategy
                                                           .getWindowStrategy(batchInterval),
                                                       PCollection.IsBounded.BOUNDED);
    }
  }

}
