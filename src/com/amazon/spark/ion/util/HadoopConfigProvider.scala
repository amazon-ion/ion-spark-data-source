// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.util

import org.apache.hadoop.conf.Configuration

/**
  * A singleton implementation that returns a single instance of Hadoop Configuration
  */
object HadoopConfigProvider {
  var conf: Option[Configuration] = Option.empty

  def getConf(s: ConfigSerDe): Configuration = {

    def loadHadoopConf(s: ConfigSerDe) = {
      this.synchronized {
        if (conf.isEmpty) { //update only if another thread has not done it already
          conf = Some(s.value)
        }
      }
    }
    if (conf.isEmpty) {
      loadHadoopConf(s)
    }
    conf.get
  }

  def init = {
    conf = Option.empty
  }
}
