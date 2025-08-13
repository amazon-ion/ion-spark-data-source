// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion

import com.amazon.spark.ion.datasource.IonFileFormat
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp

object IonOperations {

  /**
    *
    * @param spark Spark Session
    * @param paths Files for the data
    * @param schema Schema of the dataset
    * @param options Spark Option
    * @param bucketSpec Specification for bucketing of the data
    * @return  Dataset
    */
  def load(spark: SparkSession,
           paths: Seq[String],
           schema: Option[StructType] = None,
           options: Map[String, String] = Map.empty,
           bucketSpec: Option[BucketSpec] = None): Dataset[Row] = {
    loadIonData(spark, "com.amazon.spark.ion.datasource.IonFileFormat", paths, schema, options, bucketSpec)
  }

  /**
    *
    * @param spark Spark Session
    * @param paths Files for the data
    * @param schema Schema of the dataset
    * @param options Spark Option
    * @param bucketSpec Specification for bucketing of the data
    * @param partitionCols  List of partitioning columns. Only applicable if DataSource managed by hive, otherwise it is always empty.
    * @return  Dataset
    */
  private def loadIonData(spark: SparkSession,
                          format: String,
                          paths: Seq[String],
                          schema: Option[StructType] = None,
                          options: Map[String, String] = Map.empty,
                          bucketSpec: Option[BucketSpec] = None,
                          partitionCols: Seq[String] = Nil): Dataset[Row] = {
    spark.baseRelationToDataFrame(
      DataSource.apply(spark, className = format, paths, schema, partitionCols, bucketSpec, options).resolveRelation())
  }

  def loadWithRawIonData(spark: SparkSession,
                         paths: Seq[String],
                         schema: StructType,
                         recordBlobFieldName: String,
                         options: Map[String, String] = Map.empty,
                         bucketSpec: Option[BucketSpec] = None): Dataset[Row] = {
    val augmentedOptions = collection.mutable.Map(options.toSeq: _*)
    augmentedOptions += ("recordBlobFieldName" -> recordBlobFieldName)

    val blobFieldSchema = IonFileFormat.getBlobFieldSchema(recordBlobFieldName)
    val fields = (blobFieldSchema +: scala.collection.mutable.ArrayBuffer(schema.fields: _*)).toArray
    val updatedSchema = schema.copy(fields)

    load(spark, paths, Option(updatedSchema), augmentedOptions.toMap, bucketSpec)
  }

  def dedupeItems(spark: SparkSession, dataWithKeys: Dataset[Row]): Dataset[Array[Byte]] = {

    val groupByFunc: Row => (Long, String) = row => (row.getAs("domain_id"), row.getAs("item_id"))

    import spark.implicits._

    val groupedByPk: KeyValueGroupedDataset[(Long, String), Row] = dataWithKeys.groupByKey(groupByFunc)

    val deduped = groupedByPk
      .reduceGroups((row1: Row, row2: Row) => {
        val version1: Long = row1.getAs[Long]("version")
        val version2: Long = row2.getAs[Long]("version")

        if (version2 > version1) {
          row2
        } else if (version1 == version2) {
          val versionTime1: Timestamp = row1.getAs[Timestamp]("version_time")
          val versionTime2: Timestamp = row2.getAs[Timestamp]("version_time")
          if (versionTime2.after(versionTime1))
            row2
          else
            row1
        } else
          row1
      })
      .map(x => x._2.getAs[Array[Byte]]("data"))

    deduped
  }

  def saveAsText(data: DataFrame,
                 partitionByColumns: Seq[String],
                 options: Map[String, String],
                 path: String,
                 saveMode: SaveMode = SaveMode.Overwrite): Unit = {

    val optionWithSerialization = options + ("serialization" -> "text")
    save(data, partitionByColumns, optionWithSerialization, path, saveMode)
  }

  def saveAsBinary(data: DataFrame,
                   partitionByColumns: Seq[String],
                   options: Map[String, String],
                   path: String,
                   saveMode: SaveMode = SaveMode.Overwrite): Unit = {

    val optionWithSerialization = options + ("serialization" -> "binary")
    save(data, partitionByColumns, optionWithSerialization, path, saveMode)
  }

  private def save(data: DataFrame,
                   partitionByColumns: Seq[String],
                   optionWithSerialization: Map[String, String],
                   path: String,
                   saveMode: SaveMode = SaveMode.Overwrite): Unit = {

    var writer = data.write.options(optionWithSerialization)
    if (partitionByColumns.nonEmpty) {
      writer = writer.partitionBy(partitionByColumns: _*)
    }
    writer
      .format("com.amazon.spark.ion.datasource.IonFileFormat")
      .options(optionWithSerialization)
      .mode(saveMode)
      .save(path)
  }

}
