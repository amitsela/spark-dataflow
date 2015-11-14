package com.cloudera.dataflow.io;

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.common.base.Preconditions;

import com.cloudera.dataflow.spark.streaming.SparkStreamingWindowStrategy;

import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.Map;
import java.util.Set;

import kafka.serializer.Decoder;

/**
 * Read stream from Kafka
 */
public final class KafkaIO {

  private KafkaIO() {
  }

  public static final class Read {

    private Read() {
    }

    /**
     * Define the Kafka consumption
     *
     * @param keyDecoder    {@link Decoder} to decode the Kafka message key
     * @param valueDecoder  {@link Decoder} to decode the Kafka message value
     * @param key           Kafka message key Class
     * @param value         Kafka message value Class
     * @param topics        Kafka topics to subscribe
     * @param kafkaParams   map of Kafka parameters to use with {@link KafkaUtils}
     * @param batchInterval Spark streaming batch interval for {@link SparkStreamingWindowStrategy}
     * @param <K>           Kafka message key Class type
     * @param <V>           Kafka message value Class type
     * @return KafkaIO Unbound input
     */
    public static <K, V> Unbound<K, V> from(Class<? extends Decoder<K>> keyDecoder,
                                            Class<? extends Decoder<V>> valueDecoder, Class<K> key,
                                            Class<V> value, Set<String> topics,
                                            Map<String, String> kafkaParams, Long batchInterval) {
      return new Unbound<>(keyDecoder, valueDecoder, key, value, topics, kafkaParams,
                           batchInterval);
    }

    public static class Unbound<K, V> extends PTransform<PInput, PCollection<KV<K, V>>> {

      private final Class<? extends Decoder<K>> keyDecoderClass;
      private final Class<? extends Decoder<V>> valueDecoderClass;
      private final Class<K> keyClass;
      private final Class<V> valueClass;
      private final Set<String> topics;
      private final Map<String, String> kafkaParams;
      private final Long batchInterval;

      Unbound(Class<? extends Decoder<K>> keyDecoder,
              Class<? extends Decoder<V>> valueDecoder, Class<K> key,
              Class<V> value, Set<String> topics, Map<String, String> kafkaParams,
              Long batchInterval) {
        Preconditions.checkNotNull(keyDecoder,
                                   "need to set the key decoder class of a KafkaIO.Read transform");
        Preconditions.checkNotNull(valueDecoder,
                                   "need to set the value decoder class of a KafkaIO.Read transform");
        Preconditions.checkNotNull(key,
                                   "need to set the key class of aKafkaIO.Read transform");
        Preconditions.checkNotNull(value,
                                   "need to set the value class of a KafkaIO.Read transform");
        Preconditions.checkNotNull(topics,
                                   "need to set the topics of a KafkaIO.Read transform");
        Preconditions.checkNotNull(kafkaParams,
                                   "need to set the kafkaParams of a KafkaIO.Read transform");
        Preconditions.checkNotNull(batchInterval,
                                   "need to set the batchInterval of a KafkaIO.Read transform");
        this.keyDecoderClass = keyDecoder;
        this.valueDecoderClass = valueDecoder;
        this.keyClass = key;
        this.valueClass = value;
        this.topics = topics;
        this.kafkaParams = kafkaParams;
        this.batchInterval = batchInterval;
      }

      public Class<? extends Decoder<K>> getKeyDecoderClass() {
        return keyDecoderClass;
      }

      public Class<? extends Decoder<V>> getValueDecoderClass() {
        return valueDecoderClass;
      }

      public Class<V> getValueClass() {
        return valueClass;
      }

      public Class<K> getKeyClass() {
        return keyClass;
      }

      public Set<String> getTopics() {
        return topics;
      }

      public Map<String, String> getKafkaParams() {
        return kafkaParams;
      }

      @Override
      public PCollection<KV<K, V>> apply(PInput input) {
        // Spark streaming micro batches are bounded by default
        return PCollection.createPrimitiveOutputInternal(input.getPipeline(),
                                                         SparkStreamingWindowStrategy
                                                             .getWindowStrategy(batchInterval),
                                                         PCollection.IsBounded.BOUNDED);
      }
    }

  }
}
