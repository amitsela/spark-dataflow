package com.cloudera.dataflow.io;

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.PCollection;
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

    public static Unbound from() {
      return new Unbound(10);
    }

    public static Unbound from(int num) {
      return new Unbound(num);
    }

    public static class Unbound extends PTransform<PInput, PCollection<Void>> {

      private final int num;

      Unbound(int num) {
        this.num = num;
      }

      public int getNum() {
        return num;
      }

      @Override
      public PCollection<Void> apply(PInput input) {
        return PCollection.createPrimitiveOutputInternal(input.getPipeline(),
                                                         WindowingStrategy.globalDefault(),
                                                         PCollection.IsBounded.UNBOUNDED);
      }
    }
  }
}
