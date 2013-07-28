package com.metamx.druid.aggregation

import com.metamx.druid.processing.FloatMetricSelector

class TestFloatMetricSelector(floats: Array[Float]) extends FloatMetricSelector {
  private var index = 0

  def get() = floats(index)
  def increment() { index += 1 }

}
