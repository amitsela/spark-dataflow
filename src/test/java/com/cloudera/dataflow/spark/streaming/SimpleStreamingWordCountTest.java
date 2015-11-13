package com.cloudera.dataflow.spark.streaming;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.ImmutableSet;

import com.cloudera.dataflow.io.CreateStream;
import com.cloudera.dataflow.spark.EvaluationResult;
import com.cloudera.dataflow.spark.SimpleWordCountTest;
import com.cloudera.dataflow.spark.SparkPipelineRunner;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SimpleStreamingWordCountTest {

  private static final String[] WORDS_ARRAY = {
      "hi there", "hi", "hi sue bob",
      "hi sue", "", "bob hi"};
  private static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);
  private static final List<Iterable<String>> WORDS_QUEUE = Collections.<Iterable<String>>singletonList(WORDS);
  private static final Set<String> EXPECTED_COUNT_SET =
      ImmutableSet.of("hi: 5", "there: 1", "sue: 2", "bob: 2");

  @Test
  public void testRun() throws Exception {
    SparkStreamingPipelineOptions options = SparkStreamingPipelineOptionsFactory.create();
    options.setAppName(this.getClass().getSimpleName());
    options.setRunner(SparkPipelineRunner.class);
    options.setBatchInterval(1000L);
    options.setTimeout(1000L);// run for one interval
    Pipeline p = Pipeline.create(options);

    PCollection<String> inputWords =
        p.apply(CreateStream.fromQueue(WORDS_QUEUE, options.getBatchInterval())).setCoder(
            StringUtf8Coder.of());

    PCollection<String> output = inputWords.apply(new SimpleWordCountTest.CountWords());

    DataflowAssert.that(output).containsInAnyOrder(EXPECTED_COUNT_SET);

    EvaluationResult res = SparkPipelineRunner.create(options).run(p);
    res.close();
  }
}