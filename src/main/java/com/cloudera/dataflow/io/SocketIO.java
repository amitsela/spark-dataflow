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
 * Read stream from socket
 */
public class SocketIO {

  private SocketIO() {
  }

  public static final class Read {

    private Read() {
    }

    /**
     * Define the socket to listen to
     *
     * @param host          Socket host
     * @param port          Socket port
     * @param batchInterval Spark streaming batch interval for {@link SparkStreamingWindowStrategy}
     * @return SocketIO Unbounded input
     */
    public static Unbound from(String host, Integer port, Long batchInterval) {
      return new Unbound(host, port, batchInterval);
    }

    public static class Unbound extends PTransform<PInput, PCollection<String>> {

      private final String host;
      private final Integer port;
      private final Long batchInterval;

      Unbound(String host, Integer port, Long batchInterval) {
        Preconditions.checkNotNull(host,
                                   "need to set the host of a SocketIO.Read transform");
        Preconditions.checkNotNull(port,
                                   "need to set the port of a SocketIO.Read transform");
        Preconditions.checkNotNull(batchInterval,
                                   "need to set the batchInterval of a SocketIO.Read transform");
        this.host = host;
        this.port = port;
        this.batchInterval = batchInterval;
      }


      public String getHost() {
        return host;
      }

      public Integer getPort() {
        return port;
      }

      @Override
      public PCollection<String> apply(PInput input) {
        // Spark streaming micro batches are bounded by default
        return PCollection.createPrimitiveOutputInternal(input.getPipeline(),
                                                         SparkStreamingWindowStrategy
                                                             .getWindowStrategy(batchInterval),
                                                         PCollection.IsBounded.BOUNDED);
      }
    }

  }
}
