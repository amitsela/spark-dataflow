package com.cloudera.dataflow.io;

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.common.base.Preconditions;

import com.cloudera.dataflow.spark.streaming.SparkStreamingWindowStrategy;

/**
 * Created by amit on 10/13/15.
 */
public final class Create<T> {

  private Create() {
  }

  public static <T> QueuedValues<T> fromQueue(Iterable<Iterable<T>> queueStream, Long batchInterval) {
    return new QueuedValues<>(queueStream, batchInterval);
  }

  public static final class QueuedValues<T> extends PTransform<PInput, PCollection<T>> {

    private final Iterable<Iterable<T>> queueStream;
    private final Long batchInterval;

    QueuedValues(Iterable<Iterable<T>> queueStream, Long batchInterval) {
      Preconditions.checkNotNull(queueStream,
                                 "need to set the queueStream of an Create.QueuedValues transform");
      Preconditions.checkNotNull(batchInterval,
                                 "need to set the batchInterval of a SocketIO.Read transform");
      this.queueStream = queueStream;
      this.batchInterval = batchInterval;
    }

    public Iterable<Iterable<T>> queueStream() {
      return queueStream;
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
