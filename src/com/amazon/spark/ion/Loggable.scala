// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion

import org.slf4j.Logger
import org.slf4j.LoggerFactory.getLogger

trait Loggable {

  val log: Logger = getLogger(this.getClass)

}
