package com.metamx.druid.aggregation

import com.metamx.druid.processing.{FloatMetricSelector, ObjectColumnSelector, ColumnSelectorFactory}
import java.nio.ByteBuffer
import com.google.common.primitives.Ints
import java.util.Comparator
import java.util


class MonoidAggregatorFactory[T](name: String,
                                fieldName: String,
                                cacheTypeId: Byte,
                                m: Monoid[T])(implicit val ordering: Ordering[T], val codec: BufferCodec[T] with FloatRepresentation[T]) extends AggregatorFactory {


  final val Comparator = new Comparator[T] {
    def compare(o1: T, o2: T) = ordering.compare(o1, o2)
  }

  def getComparator = Comparator

  def factorize(metricFactory: ColumnSelectorFactory): Aggregator = throw new UnsupportedOperationException(this + " does not old aggregator interface")

  def factorizeBuffered(metricFactory: ColumnSelectorFactory): BufferAggregator =
    new MonoidBufferAggregator(metricFactory.makeFloatMetricSelector(fieldName), m)

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

class MonoidBufferAggregator[T](selector: FloatMetricSelector, m: Monoid[T])(implicit val codec: BufferCodec[T] with FloatRepresentation[T]) extends BufferAggregator {
  def init(buf: ByteBuffer, position: Int) {
    codec.write(buf, position, m.identity)
  }

  def aggregate(buf: ByteBuffer, position: Int) {
    val a = codec.read(buf, position)
    val b = codec.fromFloat(selector.get())
    val value = m(a, b)
    codec.write(buf, position, value)
  }

  def get(buf: ByteBuffer, position: Int): AnyRef = codec.read(buf, position).asInstanceOf[AnyRef]

  def getFloat(buf: ByteBuffer, position: Int): Float = {
    val value = codec.read(buf, position)
    codec.toFloat(value)
  }

  def close() { }
}

