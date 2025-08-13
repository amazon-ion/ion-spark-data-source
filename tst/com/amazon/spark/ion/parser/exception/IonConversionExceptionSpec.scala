// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.parser.exception

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IonConversionExceptionSpec extends AnyFlatSpec with Matchers {
  behavior of "IonConversionException"

  it should "set its message and cause on the underlying IllegalArgumentException" in {
    val cause = new Exception("test-exception")
    val exception = new IonConversionException("message", cause)
    exception.isInstanceOf[IllegalArgumentException] shouldBe true
    exception.getMessage shouldBe "message"
    exception.getCause shouldBe cause
  }

  it should "default to null cause if none is set" in {
    val exception = new IonConversionException("message")
    exception.isInstanceOf[IllegalArgumentException] shouldBe true
    exception.getMessage shouldBe "message"
    exception.getCause shouldBe null
  }
}
