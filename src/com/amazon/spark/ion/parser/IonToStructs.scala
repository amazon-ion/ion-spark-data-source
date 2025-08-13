// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.parser

import com.amazon.spark.ion.datasource.IonOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, UnaryExpression}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, TreeNode}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.annotation.tailrec
import com.fasterxml.jackson.module.scala.deser.overrides

case class IonToStructs(schema: StructType, child: Expression, options: Map[String, String] = Map.empty)
    extends UnaryExpression
    with CodegenFallback {
  override def nullable: Boolean = true

  val isIonDatagram: Boolean = options.get("isIonDatagram").exists(_.toBoolean)
  val finalSchema: DataType = if (isIonDatagram) new ArrayType(schema, true) else schema

  @transient lazy val ionExtractor = new IonExtractor(schema)
  @transient lazy val ionConverter: IonConverter = IonConverter(schema, new IonOptions(options))

  val inputTypes: Seq[Seq[DataType]] = (BinaryType :: StringType :: Nil) :: Nil

  private val isExtractionRequired = IonExtractor.isExtractionRequired(schema)

  override def checkInputDataTypes(): TypeCheckResult = {
    val mismatches = children.zip(inputTypes).zipWithIndex.collect {
      case ((input, expected), idx) if !expected.contains(input.dataType) =>
        s"argument ${idx + 1} requires $expected type, " +
          s"however, '${input.sql}' is of ${input.dataType.catalogString} type."
    }

    if (mismatches.isEmpty) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(mismatches.mkString(" "))
    }
  }

  override def nullSafeEval(ion: Any): Any = {
    val result = parseIon(ion)
    finalSchema match {
      case _: StructType if result.nonEmpty => result.head
      case _: ArrayType                     => new GenericArrayData(result)
      case _                                => null
    }
  }

  @tailrec
  final private def parseIon(ion: Any): Array[InternalRow] = {
    ion match {
      case str: UTF8String => parseIon(str.getBytes)
      case arr: Array[Byte] if isExtractionRequired =>
        ionExtractor.extractData(arr).map(ionConverter.parse).flatMap(_.toIterator).toArray
      case arr: Array[Byte] => ionConverter.parse(arr)
    }
  }

  override def dataType: DataType = finalSchema

  override def prettyName: String = "from_ion"

  override protected def withNewChildInternal(newChild: Expression): IonToStructs = copy(child = newChild)
}

object IonToStructs {
  def apply(schema: Expression, child: Expression, options: Map[String, String]): IonToStructs =
    IonToStructs(validateSchemaLiteral(schema), child, options)

  def apply(schema: Expression, child: Expression, options: Expression): IonToStructs =
    IonToStructs(validateSchemaLiteral(schema), child, validateOptionsMap(options))

  /**
    * Expression "exp" must be a string literal (leaf expression representing a Scala value of type string),
    * from which the actual string value can be extracted with pattern matching, then validated and parsed to
    * Spark schema (StructType) with CatalystSqlParser.parseTableSchema().
    * https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/literals.scala#L329
    */
  private def validateSchemaLiteral(exp: Expression): StructType = exp match {
    case Literal(s, StringType) =>
      val sparkSchema = CatalystSqlParser.parseTableSchema(s.toString)
      DdlMetadataExtractor.createSchemaWithUpdatedMetadata(sparkSchema)
    case e => throw new IllegalArgumentException(s"Expected a string literal instead of $e")
  }

  /**
    * Expression "exp" must have dataType=MapType(StringType, StringType). The actual Map[String, String] can
    * then be extracted by evaluating the expression against an empty row, giving an instance of ArrayBasedMapData
    * class, then using the ArrayBasedMapData singleton object to convert it into a Scala map.
    * Note that the runtime type of "exp" is CreateMap.
    * https://spark.apache.org/docs/latest/api/sql/index.html#map
    * https://github.com/apache/spark/blob/5c8a141d03ddd6da33b27f417b4c78c7fc6c3c28/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/complexTypeCreator.scala#L181
    * https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-Expression.html
    * https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/ArrayBasedMapData.scala
    */
  private def validateOptionsMap(exp: Expression): Map[String, String] = exp.dataType match {
    case MapType(StringType, StringType, false) =>
      val data = exp.eval().asInstanceOf[ArrayBasedMapData]
      val utf8Map = ArrayBasedMapData.toScalaMap(data).asInstanceOf[Map[UTF8String, UTF8String]]
      utf8Map map { case (k, v) => (k.toString, v.toString) }
    case e => throw new IllegalArgumentException(s"Expected a map instead of of $e")
  }

}
