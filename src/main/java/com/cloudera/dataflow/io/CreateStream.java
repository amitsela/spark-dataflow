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
      return PCollection.createPrimitiveOutputInternal(input.getPipeline(),
                                                       SparkStreamingWindowStrategy
                                                           .getWindowStrategy(batchInterval),
                                                       PCollection.IsBounded.BOUNDED);
    }
  }

}
