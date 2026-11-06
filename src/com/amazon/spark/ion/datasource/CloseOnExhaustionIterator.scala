// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.datasource

import java.io.Closeable

// Same close-on-exhaustion contract as Spark's RecordReaderIterator, which every built-in file source uses:
// https://github.com/apache/spark/blob/v4.1.1/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/RecordReaderIterator.scala
private[datasource] class CloseOnExhaustionIterator[T](private[this] var underlying: Iterator[T],
                                                       private[this] var resource: Closeable)
    extends Iterator[T]
    with Closeable {

  override def hasNext: Boolean = {
    val more = underlying != null && underlying.hasNext
    if (!more) close()
    more
  }

  override def next(): T = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    underlying.next()
  }

  override def close(): Unit = {
    if (resource != null) {
      try {
        resource.close()
      } finally {
        resource = null
        underlying = null
      }
    }
  }
}
