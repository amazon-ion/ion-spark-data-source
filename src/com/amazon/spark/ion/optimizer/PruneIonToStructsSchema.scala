// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.optimizer

import com.amazon.spark.ion.parser.IonToStructs
import org.apache.spark.sql.catalyst.expressions.GetStructField
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
  * Prunes top-level schema used by IonToStructs to only the to required fields. This helps optimize the jobs because
  * only the pruned fields will be converted from Ion to Spark DataTypes.
  */
object PruneIonToStructsSchema extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    /*
    Current implementation optimizes each occurrence of GetStructField(IonToStructs) in isolation. This will cause
    Spark's subexpression elimination to not kick in. After this optimizer is applied, all occurrences of IonToStructs
    that are used to extract different fields won't have the same schema thus making them unequal. subexpression
    elimination eliminates identical Expressions withing an operator which won't be the case until we implement below
    task. Implication of this is that the Ion column will be processed once for each field accessed instead of
    extracting all fields accessed in a single pass and then reusing it across the operator.
     */
    // TODO: Optimize IonToStructs for each LogicalPlan such that we can leverage subexpression elimination
    ((plan: QueryPlan[LogicalPlan]) transformAllExpressions {
      case gsf: GetStructField if gsf.child.isInstanceOf[IonToStructs] && gsf.childSchema.size > 1 =>
        val field = gsf.childSchema.apply(gsf.ordinal)
        val prunedSchema = gsf.childSchema.copy(Array(field))
        val childExpr = gsf.child.asInstanceOf[IonToStructs]
        val prunedIonToStructs = childExpr.copy(prunedSchema)
        gsf.copy(child = prunedIonToStructs, 0)
    }).asInstanceOf[LogicalPlan]
  }
}
