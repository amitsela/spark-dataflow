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
package com.cloudera.dataflow.io;

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.common.base.Preconditions;

/**
 * Read stream from socket.
 */
public final class SocketIO {

  private SocketIO() {
  }

  public static final class Read {

    private Read() {
    }

    /**
     * Define the socket to listen to.
     *
     * @param host          Socket host
     * @param port          Socket port
     * @return SocketIO Unbounded input
     */
    public static Unbound from(String host, Integer port) {
      return new Unbound(host, port);
    }

    public static class Unbound extends PTransform<PInput, PCollection<String>> {

      private final String host;
      private final Integer port;

      Unbound(String host, Integer port) {
        Preconditions.checkNotNull(host,
                "need to set the host of a SocketIO.Read transform");
        Preconditions.checkNotNull(port,
                "need to set the port of a SocketIO.Read transform");
        this.host = host;
        this.port = port;
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
                WindowingStrategy.globalDefault(), PCollection.IsBounded.UNBOUNDED);
      }
    }

  }
}
