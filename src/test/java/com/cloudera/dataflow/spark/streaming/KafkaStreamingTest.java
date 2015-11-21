package com.cloudera.dataflow.spark.streaming;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.ImmutableMap;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.ImmutableSet;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import com.cloudera.dataflow.io.KafkaIO;
import com.cloudera.dataflow.spark.EvaluationResult;
import com.cloudera.dataflow.spark.SparkPipelineRunner;
import com.cloudera.dataflow.spark.streaming.utils.DataflowAssertStreaming;
import com.cloudera.dataflow.spark.streaming.utils.EmbeddedKafkaCluster;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import kafka.serializer.StringDecoder;

/**
 * Test Kafka as input.
 */
public class KafkaStreamingTest {
  private static final EmbeddedKafkaCluster.EmbeddedZookeeper EMBEDDED_ZOOKEEPER =
          new EmbeddedKafkaCluster.EmbeddedZookeeper(17001);
  private static final EmbeddedKafkaCluster EMBEDDED_KAFKA_CLUSTER =
          new EmbeddedKafkaCluster(EMBEDDED_ZOOKEEPER.getConnection(),
                  new Properties(), Collections.singletonList(6667));
  private static final String TOPIC = "kafka_dataflow_test_topic";
  private static final Map<String, String> KAFKA_MESSAGES = ImmutableMap.of(
          "k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4"
  );
  private static final Set<String> EXPECTED = ImmutableSet.of(
          "k1,v1", "k2,v2", "k3,v3", "k4,v4"
  );
  private final static long TEST_INTERVAL_MSEC = 1000L;

  @BeforeClass
  public static void init() throws IOException, InterruptedException {
    EMBEDDED_ZOOKEEPER.startup();
    EMBEDDED_KAFKA_CLUSTER.startup();

    // write to Kafka
    Properties producerProps = new Properties();
    producerProps.putAll(EMBEDDED_KAFKA_CLUSTER.getProps());
    producerProps.put("request.required.acks", 1);
    producerProps.put("bootstrap.servers", EMBEDDED_KAFKA_CLUSTER.getBrokerList());
    Serializer<String> stringSerializer = new StringSerializer();
    @SuppressWarnings("unchecked") KafkaProducer<String, String> kafkaProducer =
            new KafkaProducer(producerProps, stringSerializer, stringSerializer);
    for (Map.Entry<String, String> en : KAFKA_MESSAGES.entrySet()) {
      kafkaProducer.send(new ProducerRecord<>(TOPIC, en.getKey(), en.getValue()));
    }
    kafkaProducer.close();
  }

  @Test
  public void testRun() throws Exception {
    // test read from Kafka
    SparkStreamingPipelineOptions options = SparkStreamingPipelineOptionsFactory.create();
    options.setAppName(this.getClass().getSimpleName());
    options.setRunner(SparkPipelineRunner.class);
    options.setBatchInterval(TEST_INTERVAL_MSEC);
    options.setTimeout(TEST_INTERVAL_MSEC);// run for one interval
    Pipeline p = Pipeline.create(options);

    Map<String, String> kafkaParams = ImmutableMap.of(
            "metadata.broker.list", EMBEDDED_KAFKA_CLUSTER.getBrokerList(),
            "auto.offset.reset", "smallest"
    );

    PCollection<KV<String, String>> kafkaInput = p.apply(KafkaIO.Read.from(StringDecoder.class,
            StringDecoder.class, String.class, String.class, Collections.singleton(TOPIC),
            kafkaParams, options.getBatchInterval()));

    PCollection<String> formattedKV = kafkaInput.apply(ParDo.of(new FormatKVFn()));

    DataflowAssert.that(formattedKV).containsInAnyOrder(EXPECTED);

    EvaluationResult res = SparkPipelineRunner.create(options).run(p);
    res.close();

    DataflowAssertStreaming.assertNoFailures(res);
  }

  @AfterClass
  public static void tearDown() {
    EMBEDDED_KAFKA_CLUSTER.shutdown();
    EMBEDDED_ZOOKEEPER.shutdown();
  }

  private static class FormatKVFn extends DoFn<KV<String, String>, String> {
    @Override
    public void processElement(ProcessContext c) {
      c.output(c.element().getKey() + "," + c.element().getValue());
    }
  }

}
