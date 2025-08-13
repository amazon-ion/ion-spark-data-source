// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.util

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration

/**
  * Copied from org.apache.spark.util.SerializableConfiguration
  */
private[ion] class ConfigSerDe(@transient var value: Configuration) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = {
    try {
      out.defaultWriteObject()
      value.write(out)
    } catch {
      case e: Exception =>
        println("Exception encountered serializing configuration", e)
        throw new IOException(e)
    }
  }

  private def readObject(in: ObjectInputStream): Unit = {
    try {
      value = new Configuration(false)
      value.readFields(in)
    } catch {
      case e: Exception =>
        println("Exception encountered deserializing configuration", e)
        throw new IOException(e)
    }
  }
}
