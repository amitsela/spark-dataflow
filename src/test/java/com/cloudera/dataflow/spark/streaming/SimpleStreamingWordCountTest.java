package com.cloudera.dataflow.spark.streaming;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.ImmutableSet;

import com.cloudera.dataflow.io.ConsoleIO;
import com.cloudera.dataflow.io.Create;
import com.cloudera.dataflow.spark.EvaluationResult;
import com.cloudera.dataflow.spark.SimpleWordCountTest;
import com.cloudera.dataflow.spark.SparkPipelineRunner;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class SimpleStreamingWordCountTest {

  private static final String[] WORDS_ARRAY = {
      "hi there", "hi", "hi sue bob",
      "hi sue", "", "bob hi"};
  private static final String[] WORDS_ARRAY2 = {
      "there there", "hi", "hi sue bob",
      "hi sue", "", "bob hi"};
  private static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);
  private static final List<String> WORDS2 = Arrays.asList(WORDS_ARRAY2);
  private static final Set<String> EXPECTED_COUNT_SET =
      ImmutableSet.of("hi: 0", "there: 0", "sue: 0", "bob: 0");
  private static final Set<String> EXPECTED_COUNT_SET2 =
      ImmutableSet.of("hi: -1", "there: 0", "sue: 0", "bob: 0");

  @Test
  public void testRun() throws Exception {
    SparkStreamingPipelineOptions options = SparkStreamingPipelineOptionsFactory.create();
    options.setAppName(this.getClass().getSimpleName());
    options.setRunner(SparkPipelineRunner.class);
    options.setBatchInterval(1000L);
    options.setTimeout(2000L);// run for two intervals
    Pipeline p = Pipeline.create(options);
    PCollection<String>
        inputWords =
        p.apply(Create.fromQueue(getStreamInput(), options.getBatchInterval()))
            .setCoder(
                StringUtf8Coder.of());
    PCollection<String> output = inputWords.apply(new SimpleWordCountTest.CountWords());
    output.apply(ConsoleIO.Write.from());
    EvaluationResult res = SparkPipelineRunner.create(options).run(p);
    res.close();
  }

  private Iterable<Iterable<String>> getStreamInput() {
    List<Iterable<String>> lists = new ArrayList<>();
    lists.add(WORDS);
    lists.add(WORDS2);
    return lists;
  }
}