package com.metamx.druid.aggregation

import com.metamx.druid.index.v1.serde.{ComplexMetricExtractor, ComplexMetricSerde}
import com.metamx.druid.kv.{GenericIndexed, ObjectStrategy}
import java.nio.ByteBuffer
import com.metamx.druid.index.column.ColumnBuilder
import com.metamx.druid.index.serde.{ComplexColumnPartSerde, ComplexColumnPartSupplier, ColumnPartSerde}

/**
 * Generalized complex metric serde.
 *
 * @param typeName serde registration name
 * @param extractor function to extract a T from the string representation of the metric
 * @param objectCodec codec for converting to / from ByteBuffers
 * @tparam T the complex metric type
 */
case class MetricSerde[T <: AnyRef](typeName: String, extractor: String => T, objectCodec: ObjectCodec[T])(implicit mf: Manifest[T]) extends ComplexMetricSerde {
  final val getTypeName: String = typeName
  final val getObjectStrategy: ObjectStrategy[T] = objectCodec
  final val getExtractor: ComplexMetricExtractor = new MetricExtractor[T](extractor)

  def deserializeColumn(buffer: ByteBuffer, builder: ColumnBuilder): ColumnPartSerde = {
    val column = GenericIndexed.read[T](buffer, getObjectStrategy)
    builder.setComplexColumn(new ComplexColumnPartSupplier(typeName, column))
    new ComplexColumnPartSerde(column, typeName)
  }

}

class ObjectCodec[T](codec: BufferCodec[T])(implicit ordering: Ordering[T], m: Manifest[T]) extends ObjectStrategy[T] {
  def getClazz: Class[_ <: T] = m.erasure.asInstanceOf[Class[T]]

  def fromByteBuffer(buffer: ByteBuffer, numBytes: Int): T = codec.read(buffer, position = buffer.position())

  def toBytes(value: T): Array[Byte] = {
    val buffer = ByteBuffer.allocate(codec.byteSize)
    codec.write(buffer, position = 0, value = value)
    buffer.array()
  }

  def compare(o1: T, o2: T): Int = ordering.compare(o1, o2)
}

class MetricExtractor[T <: AnyRef](private final val extractor: String => T)(implicit m: Manifest[T]) extends ComplexMetricExtractor {
  def extractedClass(): Class[_] = m.erasure.asInstanceOf[Class[T]]

  def extractValue(inputRow: com.metamx.druid.input.InputRow, metricName: String): AnyRef = {
    val stringValue = inputRow.getDimension(metricName).get(0)
    extractor(stringValue)
  }
}