package com.cloudera.dataflow.io;

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PInput;

/**
 * Print to console
 */
public final class ConsoleIO {

  private ConsoleIO() {
  }

  public static final class Write {

    private Write() {
    }

    public static <T> Unbound<T> from() {
      return new Unbound<>(10);
    }

    public static <T> Unbound<T> from(int num) {
      return new Unbound<>(num);
    }

    public static class Unbound<T> extends PTransform<PCollection<T>, PDone> {

      private final int num;

      Unbound(int num) {
        this.num = num;
      }

      public int getNum() {
        return num;
      }

      @Override
      public PDone apply(PCollection<T> input) {
        return PDone.in(input.getPipeline());
      }
    }
  }
}
