package com.metamx.druid.aggregation

import org.junit._
import java.nio.ByteBuffer

class MonoidAggregatorTest {
  private def aggregate[T](selector: TestSelector[T], agg: BufferAggregator)(implicit buffer: ByteBuffer) {
    agg.aggregate(buffer, 0)
    selector.increment()
  }

  @Test def testAggregate() {
    val selector = new TestSelector(Array(24, 20))
    val agg = new MonoidBufferAggregator[Int](selector, Sum)

    implicit val buffer = ByteBuffer.allocate(agg.codec.byteSize)

    Assert.assertEquals(0, agg.get(buffer, 0))
    Assert.assertEquals(0, agg.get(buffer, 0))
    aggregate(selector, agg)
    Assert.assertEquals(24, agg.get(buffer, 0))
    aggregate(selector, agg)
    Assert.assertEquals(44, agg.get(buffer, 0))
  }
}
