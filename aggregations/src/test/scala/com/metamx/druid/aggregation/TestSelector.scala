package com.metamx.druid.aggregation

import com.metamx.druid.processing.{ComplexMetricSelector, FloatMetricSelector}

class TestSelector[T](values: Array[T])(implicit manifest: Manifest[T]) extends ComplexMetricSelector[T] {
  private var index = 0


  def classOfObject(): Class[T] = manifest.erasure.asInstanceOf[Class[T]]

  def get() = values(index)
  def increment() { index += 1 }

}
