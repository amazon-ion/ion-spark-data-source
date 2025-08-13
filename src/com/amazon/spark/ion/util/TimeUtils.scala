// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.util

import java.util.TimeZone

private[ion] object TimeUtils {
  final val SECONDS_PER_DAY = 60 * 60 * 24L
  final val MICROS_PER_MILLIS = 1000L
  final val MILLIS_PER_SECOND = 1000L
  final val MICROS_PER_SECOND = MICROS_PER_MILLIS * MILLIS_PER_SECOND
  final val MICROS_PER_DAY = MICROS_PER_SECOND * SECONDS_PER_DAY
  final val NANOS_PER_MICROS = 1000L
  final val MILLIS_PER_DAY = SECONDS_PER_DAY * 1000L

  def defaultTimeZone: TimeZone = TimeZone.getDefault
}
