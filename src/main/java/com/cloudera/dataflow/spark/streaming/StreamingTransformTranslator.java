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
package com.cloudera.dataflow.spark.streaming;

import com.google.api.client.util.Maps;
import com.google.api.client.util.Sets;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PDone;

import com.clearspring.analytics.util.Lists;
import com.cloudera.dataflow.hadoop.HadoopIO;
import com.cloudera.dataflow.io.ConsoleIO;
import com.cloudera.dataflow.io.CreateStream;
import com.cloudera.dataflow.spark.EvaluationContext;
import com.cloudera.dataflow.spark.SparkPipelineTranslator;
import com.cloudera.dataflow.spark.TransformEvaluator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStreamLike;

import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import scala.Tuple2;

/**
 * Supports translation between a DataFlow transform, and Spark's operations on DStreams.
 */
public final class StreamingTransformTranslator {

  private StreamingTransformTranslator() {
  }

  private static <T> TransformEvaluator<ConsoleIO.Write.Unbound<T>> print() {
    return new TransformEvaluator<ConsoleIO.Write.Unbound<T>>() {
      @Override
      public void evaluate(ConsoleIO.Write.Unbound transform, EvaluationContext context) {
        ((StreamingEvaluationContext) context).getStream(transform).print(transform.getNum());
      }
    };
  }

  private static <T> TransformEvaluator<com.google.cloud.dataflow.sdk.transforms.Create.Values<T>> create() {
    return new TransformEvaluator<com.google.cloud.dataflow.sdk.transforms.Create.Values<T>>() {
      @SuppressWarnings("unchecked")
      @Override
      public void evaluate(com.google.cloud.dataflow.sdk.transforms.Create.Values<T> transform, EvaluationContext context) {
        Iterable<T> elems = transform.getElements();
        Coder<T> coder = ((StreamingEvaluationContext) context).getOutput(transform).getCoder();
        if (coder != VoidCoder.of()) {
          // actual create
          ((StreamingEvaluationContext) context).setOutputRDDFromValues(transform, elems, coder);
        } else {
          // fake create as an input
          // creates a stream with a single batch containing a single null element
          // to invoke following transformations once
          // to support DataflowAssert
          ((StreamingEvaluationContext) context).setDStreamFromQueue(transform, Collections.<Iterable<Void>>singletonList(
                                                                         Collections.singletonList(
                                                                             (Void) null)),
                                                                     (Coder<Void>) coder);
        }
      }
    };
  }

  private static <T> TransformEvaluator<CreateStream.QueuedValues<T>> createFromQueue() {
    return new TransformEvaluator<CreateStream.QueuedValues<T>>() {
      @Override
      public void evaluate(CreateStream.QueuedValues<T> transform, EvaluationContext context) {
        Iterable<Iterable<T>> values = transform.getQueuedValues();
        Coder<T> coder = ((StreamingEvaluationContext) context).getOutput(transform).getCoder();
        ((StreamingEvaluationContext) context).setDStreamFromQueue(transform, values, coder);
      }
    };
  }

  private static <PT extends PTransform<?, ?>> TransformEvaluator<PT> rddTransform(
      final SparkPipelineTranslator rddTranslator) {
    return new TransformEvaluator<PT>() {
      @SuppressWarnings("unchecked")
      @Override
      public void evaluate(final PT transform,
                           final EvaluationContext context) {
        final TransformEvaluator
            rddEvaluator = rddTranslator.translate(
            (Class<? extends PTransform<?, ?>>) transform.getClass());
        if (((StreamingEvaluationContext) context).hasStream(transform)) {
          JavaDStreamLike<Object, ?, JavaRDD<Object>>
              dStream =
              (JavaDStreamLike<Object, ?, JavaRDD<Object>>) ((StreamingEvaluationContext) context)
                  .getStream(transform);
          ((StreamingEvaluationContext) context).setStream(transform, dStream
              .transform(new RDDTransform<>((StreamingEvaluationContext) context, rddEvaluator,
                                            transform)));
        } else {
          // if the transformation requires direct access to RDD (not in stream)
          // this is used for "fake" transformations like with DataflowAssert
          rddEvaluator.evaluate(transform, context);
        }
      }
    };
  }

  /**
   * RDD transform function
   * If the transformation function doesn't have an input, create a fake one as an empty RDD
   * @param <PT> PTransform type
   */
  private static final class RDDTransform<PT extends PTransform<?, ?>>
      implements Function<JavaRDD<Object>, JavaRDD<Object>> {

    private final StreamingEvaluationContext context;
    private final AppliedPTransform<?, ?, ?> appliedPTransform;
    private final TransformEvaluator rddEvaluator;
    private final PT transform;


    private RDDTransform(StreamingEvaluationContext context,
                         TransformEvaluator rddEvaluator, PT transform) {
      this.context = context;
      this.appliedPTransform = context.getCurrentTransform();
      this.rddEvaluator = rddEvaluator;
      this.transform = transform;
    }

    @Override
    @SuppressWarnings("unchecked")
    public JavaRDD<Object> call(JavaRDD<Object> rdd) throws Exception {
      AppliedPTransform<?, ?, ?> existingAPT = context.getCurrentTransform();
      context.setCurrentTransform(appliedPTransform);
      context.setInputRDD(transform, rdd);
      rddEvaluator.evaluate(transform, context);
      if (!context.hasOutputRDD(transform)) {
        // fake RDD as output
        context.setOutputRDD(transform, context.getSparkContext().emptyRDD());
      }
      JavaRDD<Object> outRDD = (JavaRDD<Object>) context.getOutputRDD(transform);
      context.setCurrentTransform(existingAPT);
      return outRDD;
    }
  }

  @SuppressWarnings("unchecked")
  private static <PT extends PTransform<?, ?>> TransformEvaluator<PT> foreachRDD(
      final SparkPipelineTranslator rddTranslator) {
    return new TransformEvaluator<PT>() {
      @Override
      public void evaluate(final PT transform,
                           final EvaluationContext context) {
        final TransformEvaluator
            rddEvaluator = rddTranslator.translate(
            (Class<? extends PTransform<?, ?>>) transform.getClass());
        if (((StreamingEvaluationContext) context).hasStream(transform)) {
          JavaDStreamLike<Object, ?, JavaRDD<Object>>
              dStream =
              (JavaDStreamLike<Object, ?, JavaRDD<Object>>) ((StreamingEvaluationContext) context)
                  .getStream(transform);
          dStream.foreachRDD(
              new RDDOutputOperator<>((StreamingEvaluationContext) context, rddEvaluator,
                                      transform));
        } else {
          rddEvaluator.evaluate(transform, context);
        }
      }
    };
  }

  /**
   * RDD output function
   * @param <PT> PTransform type
   */
  private static final class RDDOutputOperator<PT extends PTransform<?, ?>>
      implements Function<JavaRDD<Object>, Void> {

    private final StreamingEvaluationContext context;
    private final AppliedPTransform<?, ?, ?> appliedPTransform;
    private final TransformEvaluator rddEvaluator;
    private final PT transform;


    private RDDOutputOperator(StreamingEvaluationContext context,
                         TransformEvaluator rddEvaluator, PT transform) {
      this.context = context;
      this.appliedPTransform = context.getCurrentTransform();
      this.rddEvaluator = rddEvaluator;
      this.transform = transform;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Void call(JavaRDD<Object> rdd) throws Exception {
      AppliedPTransform<?, ?, ?> existingAPT = context.getCurrentTransform();
      context.setCurrentTransform(appliedPTransform);
      context.setInputRDD(transform, rdd);
      rddEvaluator.evaluate(transform, context);
      context.setCurrentTransform(existingAPT);
      return null;
    }
  }

  private static final Map<Class<? extends PTransform>, TransformEvaluator<?>> EVALUATORS = Maps
      .newHashMap();
  static {
    EVALUATORS.put(ConsoleIO.Write.Unbound.class, print());
    EVALUATORS.put(CreateStream.QueuedValues.class, createFromQueue());
    EVALUATORS.put(Create.Values.class, create());
  }

  private static final Set<Class<? extends PTransform>> UNSUPPORTTED_EVALUATORS = Sets
      .newHashSet();
  static {
    //TODO - add support for the following
    UNSUPPORTTED_EVALUATORS.add(TextIO.Read.Bound.class);
    UNSUPPORTTED_EVALUATORS.add(TextIO.Write.Bound.class);
    UNSUPPORTTED_EVALUATORS.add(AvroIO.Read.Bound.class);
    UNSUPPORTTED_EVALUATORS.add(AvroIO.Write.Bound.class);
    UNSUPPORTTED_EVALUATORS.add(HadoopIO.Read.Bound.class);
    UNSUPPORTTED_EVALUATORS.add(HadoopIO.Write.Bound.class);
    UNSUPPORTTED_EVALUATORS.add(Flatten.FlattenPCollectionList.class);
  }

  private static <PT extends PTransform<?, ?>> boolean hasTransformEvaluator(Class<PT> clazz) {
    return EVALUATORS.containsKey(clazz);
  }

  @SuppressWarnings("unchecked")
  private static <PT extends PTransform<?, ?>> TransformEvaluator<PT> getTransformEvaluator(Class<PT>
                                                   clazz, SparkPipelineTranslator rddTranslator) {
    TransformEvaluator<PT> transform = (TransformEvaluator<PT>) EVALUATORS.get(clazz);
    if (transform == null) {
      if (UNSUPPORTTED_EVALUATORS.contains(clazz)) {
        throw new UnsupportedOperationException("Dataflow transformation " + clazz.getCanonicalName()
                                                + " is currently unsupported by the Spark streaming pipeline");
      }
      // DStream transformations will transform an RDD into another RDD
      // Actions will create output
      // In Dataflow it depends on the PTranform's Input and Output class
      Class PTOutputClazz = getPTransformInputOutput(clazz)._2();
      if (PTOutputClazz == PDone.class) {
        return foreachRDD(rddTranslator);
      } else {
        return rddTransform(rddTranslator);
      }
    }
    return transform;
  }

  private static <PT extends PTransform<?, ?>> Tuple2<Class, Class> getPTransformInputOutput(Class<PT> clazz) {
    Type[] types = ((ParameterizedType) clazz.getGenericSuperclass()).getActualTypeArguments();
    List<Class> clazzList = Lists.newArrayList();
    for (Type type: types) {
      Class c =
          type instanceof ParameterizedTypeImpl ? ((ParameterizedTypeImpl) type).getRawType() : (Class) type;
      clazzList.add(c);
    }
    return new Tuple2<>(clazzList.get(0), clazzList.get(1));
  }

  /**
   * Translator matches Dataflow transformation with the appropriate Spark streaming evaluator
   * rddTranslator uses Spark evaluators in transform/foreachRDD to evaluate the transformation
   */
  public static class Translator implements SparkPipelineTranslator {

    private final SparkPipelineTranslator rddTranslator;

    public Translator(SparkPipelineTranslator rddTranslator) {
      this.rddTranslator = rddTranslator;
    }

    @Override
    public boolean hasTranslation(Class<? extends PTransform<?, ?>> clazz) {
      // streaming includes rdd transformations as well
      return hasTransformEvaluator(clazz) || rddTranslator.hasTranslation(clazz);
    }

    @Override
    public TransformEvaluator<? extends PTransform<?, ?>> translate(
        Class<? extends PTransform<?, ?>> clazz) {
      return getTransformEvaluator(clazz, rddTranslator);
    }
  }
}
