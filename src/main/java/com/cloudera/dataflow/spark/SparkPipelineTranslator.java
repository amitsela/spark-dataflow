package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.transforms.PTransform;

/**
 * Translator to support translation between Dataflow transformations and Spark transformations
 */
public interface SparkPipelineTranslator {

  boolean hasTranslation(Class<? extends PTransform<?, ?>> clazz);

  TransformEvaluator<? extends PTransform<?, ?>> translate(
      Class<? extends PTransform<?, ?>>
          clazz);
}
