// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.optimizer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions

class IonUnzipOptimizerRuleSqlExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    logInfo("Injecting PruneIonToStructsSchema optimization")
    extensions.injectOptimizerRule(_ => PruneIonToStructsSchema)
  }
}
