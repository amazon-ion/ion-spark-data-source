// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterEach, Suite}

trait SparkTest extends BeforeAndAfterEach { this: Suite =>

  implicit var spark: SparkSession = null
  implicit var sparkContext: SparkContext = null
  implicit var sparkSql: SQLContext = null

  override def beforeEach(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("SparkTest")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()
    sparkContext = spark.sparkContext
    sparkSql = spark.sqlContext
  }

  override def afterEach(): Unit = {
    if (spark != null) spark.stop()
    sparkContext.stop()
  }

}
