package com.cloudera.dataflow.spark.streaming.utils;

import com.cloudera.dataflow.spark.EvaluationResult;

import org.junit.Assert;

/**
 * Since DataflowAssert doesn't propagate assert exceptions, use Aggregators to assert streaming
 * success/failure counters.
 */
public final class DataflowAssertStreaming {
  /**
   * Copied aggregator names from {@link com.google.cloud.dataflow.sdk.testing.DataflowAssert}
   */
  static final String SUCCESS_COUNTER = "DataflowAssertSuccess";
  static final String FAILURE_COUNTER = "DataflowAssertFailure";

  private DataflowAssertStreaming() {
  }

  public static void assertNoFailures(EvaluationResult res) {
    int failures = res.getAggregatorValue(FAILURE_COUNTER, Integer.class);
    Assert.assertEquals("Found " + failures + " failures, see the log for details", 0,
            failures);
  }
}
