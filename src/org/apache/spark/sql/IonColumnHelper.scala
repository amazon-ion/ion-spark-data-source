// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.classic.ExpressionColumnNode

object IonColumnHelper {
  def fromExpression(expr: Expression): Column = Column(ExpressionColumnNode(expr))
}
