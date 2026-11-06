// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.datasource

import org.apache.hadoop.fs.{FSDataInputStream, LocalFileSystem, Path}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

class OpenStreamTrackingFileSystem extends LocalFileSystem {

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    val opened = super.open(f, bufferSize)
    if (!OpenStreamTrackingFileSystem.isTracked(f)) {
      opened
    } else {
      OpenStreamTrackingFileSystem.onOpen()
      new FSDataInputStream(opened) {
        private val closed = new AtomicBoolean(false)

        override def close(): Unit = {
          if (closed.compareAndSet(false, true)) OpenStreamTrackingFileSystem.onClose()
          super.close()
        }
      }
    }
  }
}

object OpenStreamTrackingFileSystem {
  val TrackedSuffix = ".ion.gz"

  private val opened = new AtomicInteger(0)
  private val open = new AtomicInteger(0)
  private val maxOpen = new AtomicInteger(0)

  def hadoopOptions: Map[String, String] =
    Map("fs.file.impl" -> classOf[OpenStreamTrackingFileSystem].getName, "fs.file.impl.disable.cache" -> "true")

  def reset(): Unit = {
    opened.set(0)
    open.set(0)
    maxOpen.set(0)
  }

  def openedCount: Int = opened.get()

  def currentlyOpen: Int = open.get()

  def maxConcurrentlyOpen: Int = maxOpen.get()

  private def isTracked(f: Path): Boolean = f.getName.endsWith(TrackedSuffix)

  private def onOpen(): Unit = {
    opened.incrementAndGet()
    val now = open.incrementAndGet()
    maxOpen.accumulateAndGet(now, math.max)
  }

  private def onClose(): Unit = open.decrementAndGet()
}
