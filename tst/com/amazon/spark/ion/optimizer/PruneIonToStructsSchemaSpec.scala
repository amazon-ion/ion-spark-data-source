// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.optimizer

import com.amazon.spark.ion.parser.IonToStructs
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, GetStructField, IsNotNull, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, Project}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should._

class PruneIonToStructsSchemaSpec extends AnyFlatSpec with Matchers {

  behavior of "PruneIonToStructsSchema"

  it should "prune schema from single IonToStructs expression when reading fieldIndex 0" in {
    val ionData = "{a:1, b:'foo', c:123}"
    val schema = StructType(
      StructField("a", IntegerType) ::
        StructField("b", StringType) ::
        StructField("c", IntegerType) :: Nil)
    val b = AttributeReference("b", StringType)()

    val plan = Filter(IsNotNull(GetStructField(IonToStructs(schema, Literal(ionData)), 0)), LocalRelation(b))
    val prunedPlan = PruneIonToStructsSchema(plan)

    val expectedPlan =
      Filter(IsNotNull(GetStructField(IonToStructs(schema.copy(Array(schema.head)), Literal(ionData)), 0)),
             LocalRelation(b))

    prunedPlan shouldBe expectedPlan
  }

  it should "prune schema from single IonToStructs expression when reading fieldIndex > 0" in {
    val ionData = "{a:1, b:'foo', c:123}"
    val schema = StructType(
      StructField("a", IntegerType) ::
        StructField("b", StringType) ::
        StructField("c", IntegerType) :: Nil)
    val b = AttributeReference("b", StringType)()

    val plan = Filter(IsNotNull(GetStructField(IonToStructs(schema, Literal(ionData)), 2)), LocalRelation(b))
    val prunedPlan = PruneIonToStructsSchema(plan)

    val expectedPlan =
      Filter(IsNotNull(GetStructField(IonToStructs(schema.copy(Array(schema.apply(2))), Literal(ionData)), 0)),
             LocalRelation(b))

    prunedPlan shouldBe expectedPlan
  }

  it should "prune schema from multiple IonToStructs expressions in single plan" in {
    val ionData = "{a:1, b:'foo', c:123}"
    val schema = StructType(
      StructField("a", IntegerType) ::
        StructField("b", StringType) ::
        StructField("c", IntegerType) :: Nil)
    val b = AttributeReference("b", StringType)()

    val plan = Filter(
      And(IsNotNull(GetStructField(IonToStructs(schema, Literal(ionData)), 2)),
          IsNotNull(GetStructField(IonToStructs(schema, Literal(ionData)), 1))),
      LocalRelation(b)
    )
    val prunedPlan = PruneIonToStructsSchema(plan)

    val expectedPlan = Filter(
      And(
        IsNotNull(GetStructField(IonToStructs(schema.copy(Array(schema.apply(2))), Literal(ionData)), 0)),
        IsNotNull(GetStructField(IonToStructs(schema.copy(Array(schema.apply(1))), Literal(ionData)), 0))
      ),
      LocalRelation(b)
    )

    prunedPlan shouldBe expectedPlan
  }

  it should "not do anything when the schema only has one field" in {
    val ionData = "{a:1, b:'foo', c:123}"
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    val b = AttributeReference("b", StringType)()

    val plan = Filter(IsNotNull(GetStructField(IonToStructs(schema, Literal(ionData)), 0)), LocalRelation(b))
    val unchangedPlan = PruneIonToStructsSchema(plan)

    unchangedPlan should be theSameInstanceAs plan
  }

  it should "not do anything if the plan does not have IonToStructs expression" in {
    val b = AttributeReference("b", StringType)()

    val plan = Filter(IsNotNull(GetStructField(Literal("foo"), 0)), LocalRelation(b))
    val unchangedPlan = PruneIonToStructsSchema(plan)

    unchangedPlan should be theSameInstanceAs plan
  }
}
