// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.optimizer

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{times, verify}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class IonUnzipOptimizerRuleSqlExtensionSpec extends AnyFlatSpec with MockitoSugar with Matchers {

  behavior of "IonUnzipOptimizerRuleSqlExtension"

  it should "inject optimizer rule PruneIonToStructsSchema" in {
    val mockSparkSessionExtensions = mock[SparkSessionExtensions]
    val extensionToTest = new IonUnzipOptimizerRuleSqlExtension
    val ruleBuilderCaptor = ArgumentCaptor.forClass(classOf[Function[SparkSession, Rule[LogicalPlan]]])
    extensionToTest.apply(mockSparkSessionExtensions)
    verify(mockSparkSessionExtensions, times(1)).injectOptimizerRule(ruleBuilderCaptor.capture())
    val injectedRuleName = ruleBuilderCaptor.getValue.getClass.getName
    injectedRuleName should not be null
    injectedRuleName should startWith(classOf[IonUnzipOptimizerRuleSqlExtension].getName)
  }
}
