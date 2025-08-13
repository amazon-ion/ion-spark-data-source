// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.parser

import com.amazon.ion.IonException
import com.amazon.spark.ion.datasource.IonOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.types.{BinaryType, DataType, MapType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.io.{ByteArrayOutputStream, IOException}

/**
  * Expression to create an Ion binary representation of a struct from the input columns.
  * The output is a an Ion binary blob containing an Ion struct with all the columns' data.
  * The input schema must be a struct type.
  *
  * @param children    list of column expressions for the input columns
  * @param inputSchema input schema of the input columns
  * @param options     optional configuration options for the IonGenerator
  * @return an Ion binary representation of a struct
  */
case class CreateIonStruct(children: Seq[Expression], inputSchema: DataType, options: Map[String, String])
    extends Expression
    with CodegenFallback {

  override def nullable: Boolean = false

  override lazy val dataType: DataType = StructType(
    Seq(
      StructField("all_attributes", BinaryType, nullable = true)
    ))

  @transient
  lazy val outputStream = new ByteArrayOutputStream()

  @transient
  lazy val ionGenerator = new IonGenerator(inputSchema.asInstanceOf[StructType], outputStream, new IonOptions(options))

  @transient
  val ionBinaryRow = new GenericInternalRow(1)

  @transient
  lazy val converter: InternalRow => InternalRow = { (row: InternalRow) =>
    try {
      ionGenerator.write(row)
      ionGenerator.flush()
      ionGenerator.finish()
      ionBinaryRow.update(0, outputStream.toByteArray)
      ionBinaryRow
    } catch {
      case ioException: IOException =>
        throw new RuntimeException("Failed to write data to OutputStream.", ioException)
      case ionException: IonException =>
        throw new RuntimeException("Failed to generate Ion binary.", ionException)
    } finally {
      outputStream.reset()
    }
  }

  override def eval(input: InternalRow): Any = {
    try {
      converter(InternalRow(children.map(_.eval(input)): _*))
    } finally {
      outputStream.close()
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    inputSchema match {
      case structSchema: StructType if !structSchema.fieldNames.contains("all_attributes") =>
        TypeCheckResult.TypeCheckSuccess
      case _: StructType =>
        TypeCheckResult.TypeCheckFailure("Input schema already has an 'all_attributes' column.")
      case _ => TypeCheckResult.TypeCheckFailure(s"Input schema ${inputSchema.typeName} must be a struct.")
    }
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}

object CreateIonStruct {
  def apply(children: Seq[Expression], inputSchema: StructType, options: Expression): CreateIonStruct =
    CreateIonStruct(children, inputSchema, validateOptionsMap(options))

  /**
    * Taken from IonToStructs Expression ->
    * Expression "exp" must have dataType=MapType(StringType, StringType). The actual Map[String, String] can
    * then be extracted by evaluating the expression against an empty row, giving an instance of ArrayBasedMapData
    * class, then using the ArrayBasedMapData singleton object to convert it into a Scala map.
    */
  private def validateOptionsMap(exp: Expression): Map[String, String] = exp.dataType match {

    case MapType(StringType, StringType, false) =>
      val data = exp.eval().asInstanceOf[ArrayBasedMapData]
      val utf8Map = ArrayBasedMapData.toScalaMap(data).asInstanceOf[Map[UTF8String, UTF8String]]
      utf8Map map { case (k, v) => (k.toString, v.toString) }

    case e => throw new IllegalArgumentException(s"Expected a map instead of of $e")
  }
}
