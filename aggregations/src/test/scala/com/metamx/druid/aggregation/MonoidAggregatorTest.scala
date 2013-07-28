package com.metamx.druid.aggregation

import org.junit._
import java.nio.ByteBuffer

class MonoidAggregatorTest {
  private def aggregate(selector: TestFloatMetricSelector, agg: BufferAggregator)(implicit buffer: ByteBuffer) {
    agg.aggregate(buffer, 0)
    selector.increment()
  }

  @Test def testAggregate() {
    val selector = new TestFloatMetricSelector(Array(24.15f, 20f))
    val agg = new MonoidBufferAggregator(selector, Sum)

    implicit val buffer = ByteBuffer.allocate(agg.codec.byteSize)

    Assert.assertEquals(0, agg.get(buffer, 0))
    Assert.assertEquals(0, agg.get(buffer, 0))
    aggregate(selector, agg)
    Assert.assertEquals(24, agg.get(buffer, 0))
    aggregate(selector, agg)
    Assert.assertEquals(44, agg.get(buffer, 0))
  }

  @Test def testAggregateAverage() {
    val selector = new TestFloatMetricSelector(Array(10f, 20f))
    val agg = new MonoidBufferAggregator(selector, Average)

    implicit val buffer = ByteBuffer.allocate(agg.codec.byteSize)

    Assert.assertEquals(Average(0, 0), agg.get(buffer, 0))
    aggregate(selector, agg)
    aggregate(selector, agg)
    Assert.assertEquals(Average(2, 30), agg.get(buffer, 0))
  }

}
