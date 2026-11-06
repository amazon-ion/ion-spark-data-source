// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.datasource

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.Closeable

class CloseOnExhaustionIteratorSpec extends AnyFlatSpec with Matchers {

  private class CountingCloseable extends Closeable {
    var closeCount = 0
    override def close(): Unit = closeCount += 1
  }

  behavior of "CloseOnExhaustionIterator"

  it should "return every element of the underlying iterator" in {
    val iterator = new CloseOnExhaustionIterator(Iterator(1, 2, 3), new CountingCloseable)

    iterator.toList shouldEqual List(1, 2, 3)
  }

  it should "not close the resource while elements remain" in {
    val resource = new CountingCloseable
    val iterator = new CloseOnExhaustionIterator(Iterator(1, 2), resource)

    iterator.hasNext shouldBe true
    iterator.next() shouldBe 1

    resource.closeCount shouldBe 0
  }

  it should "close the resource once the underlying iterator is exhausted" in {
    val resource = new CountingCloseable
    val iterator = new CloseOnExhaustionIterator(Iterator(1, 2), resource)

    iterator.toList

    resource.closeCount shouldBe 1
  }

  it should "close the resource for an empty underlying iterator" in {
    val resource = new CountingCloseable
    val iterator = new CloseOnExhaustionIterator(Iterator.empty, resource)

    iterator.hasNext shouldBe false

    resource.closeCount shouldBe 1
  }

  it should "close the resource only once across exhaustion, repeated hasNext and explicit close" in {
    val resource = new CountingCloseable
    val iterator = new CloseOnExhaustionIterator(Iterator(1), resource)

    iterator.toList
    iterator.hasNext shouldBe false
    iterator.close()
    iterator.close()

    resource.closeCount shouldBe 1
  }

  it should "throw NoSuchElementException from next once the underlying iterator is exhausted" in {
    val iterator = new CloseOnExhaustionIterator(Iterator(1), new CountingCloseable)

    iterator.next()

    a[NoSuchElementException] should be thrownBy iterator.next()
  }

  it should "throw NoSuchElementException from next after close without touching the underlying iterator" in {
    val underlying = new Iterator[Int] {
      override def hasNext: Boolean = true
      override def next(): Int = throw new IllegalStateException("underlying iterator used after close")
    }
    val iterator = new CloseOnExhaustionIterator(underlying, new CountingCloseable)

    iterator.close()

    a[NoSuchElementException] should be thrownBy iterator.next()
  }

  it should "close the resource and report no more elements when closed early" in {
    val resource = new CountingCloseable
    val iterator = new CloseOnExhaustionIterator(Iterator(1, 2, 3), resource)

    iterator.next()
    iterator.close()

    resource.closeCount shouldBe 1
    iterator.hasNext shouldBe false
  }
}
