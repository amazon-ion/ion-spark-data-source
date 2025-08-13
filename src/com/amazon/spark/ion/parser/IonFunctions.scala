// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.parser

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StructType

object IonFunctions extends Serializable {

  /**
    * Parses a column containing an Ion Byte Array of single Ion record into a `StructType` with the specified schema
    *
    * @param c      a Byte Array containing Ion record
    * @param schema the schema to use when parsing Ion data
    */
  def from_ion(c: Column, schema: StructType, options: Map[String, String] = Map.empty): Column = {
    from_any_ion(c, schema, options)
  }

  /**
    * Converts columns in native type into single Ion binary struct.
    *
    * @param cols         Sequence of native columns to zip
    * @param schema       Schema to use while generating Ion data
    * @param options      Ion Options
    * @return
    */
  def to_ion(cols: Seq[Column], schema: StructType, options: Map[String, String] = Map.empty): Column = {
    new Column(CreateIonStruct(cols.map(_.expr), schema, options))
  }

  /**
    * Parses a column containing an Ion Byte Array of one or more Ion records into a `ArrayType` with the specified schema
    *
    * @param c      a Byte Array containing Ion record(s)
    * @param schema the schema to use when parsing Ion data
    */
  def from_ion_datagram(c: Column, schema: StructType, options: Map[String, String] = Map.empty): Column = {
    from_any_ion(c, schema, options + ("isIonDatagram" -> "true"))
  }

  private def from_any_ion(c: Column, schema: StructType, options: Map[String, String] = Map.empty): Column = {
    new Column(IonToStructs(schema, c.expr, options))
  }

}
