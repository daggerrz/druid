package com.metamx.druid.aggregation

import com.metamx.druid.processing.{ComplexMetricSelector, FloatMetricSelector, ObjectColumnSelector, ColumnSelectorFactory}
import java.nio.ByteBuffer
import com.google.common.primitives.Ints
import java.util.Comparator
import java.util


class MonoidAggregatorFactory[T](name: String,
                                fieldName: String,
                                cacheTypeId: Byte,
                                m: Monoid[T])(implicit val ordering: Ordering[T], val codec: BufferCodec[T]) extends AggregatorFactory {


  final val Comparator = new Comparator[T] {
    def compare(o1: T, o2: T) = ordering.compare(o1, o2)
  }

  def getComparator = Comparator

  def factorize(metricFactory: ColumnSelectorFactory): Aggregator = {
    new MonoidAggeator[T](
      name,
      metricFactory.makeComplexMetricSelector(fieldName).asInstanceOf[ComplexMetricSelector[T]],
      m
    )
  }

  def factorizeBuffered(metricFactory: ColumnSelectorFactory): BufferAggregator = {
    new MonoidBufferAggregator(
      metricFactory.makeComplexMetricSelector(fieldName).asInstanceOf[ComplexMetricSelector[T]],
      m
    )
  }

  def combine(lhs: AnyRef, rhs: AnyRef): AnyRef = {
    m(lhs.asInstanceOf[T], rhs.asInstanceOf[T]).asInstanceOf[AnyRef]
  }

  def getCombiningFactory: AggregatorFactory = new MonoidAggregatorFactory(name, name, cacheTypeId, m)

  def deserialize(o: AnyRef): AnyRef = o

  def finalizeComputation(o: AnyRef): AnyRef = o

  def getName: String = name

  def requiredFields: java.util.List[String] = util.Arrays.asList(fieldName)

  def getCacheKey: Array[Byte] = {
    val fieldNameBytes = fieldName.getBytes
    ByteBuffer.allocate(1 + fieldNameBytes.length).put(cacheTypeId).put(fieldNameBytes).array()
  }

  def getTypeName: String = codec.typeName

  def getMaxIntermediateSize = codec.byteSize

  def getAggregatorStartValue: AnyRef = m.identity.asInstanceOf[AnyRef]

}

class MonoidAggeator[T](final val name: String, selector: ComplexMetricSelector[T], m: Monoid[T])(implicit val codec: BufferCodec[T]) extends Aggregator {
  private[this] var value = m.identity

  def aggregate() { value = m.apply(value, selector.get()) }

  def reset() { value = m.identity }

  def get(): AnyRef = value.asInstanceOf[AnyRef]

  def getName: String = name

  override def clone = new MonoidAggeator[T](name, selector, m)

  def getFloat: Float = throw new UnsupportedOperationException("This aggregator only supports complex metrics")

  def close() {}
}

class MonoidBufferAggregator[T](selector: ComplexMetricSelector[T], m: Monoid[T])(implicit val codec: BufferCodec[T]) extends BufferAggregator {
  def init(buf: ByteBuffer, position: Int) {
    codec.write(buf, position, m.identity)
  }

  def aggregate(buf: ByteBuffer, position: Int) {
    val a = codec.read(buf, position)
    val selected = selector.get()
    val value = m(a, selected)
    codec.write(buf, position, value)
  }

  def get(buf: ByteBuffer, position: Int): AnyRef = codec.read(buf, position).asInstanceOf[AnyRef]

  def getFloat(buf: ByteBuffer, position: Int): Float = throw new UnsupportedOperationException("This aggregator only supports complex metrics")

  def close() { }
}

