package com.metamx.druid.aggregation

import java.nio.ByteBuffer
import com.google.common.primitives.Ints

trait BufferCodec[T] {
  def read(buf: ByteBuffer, position: Int): T

  def write(buf: ByteBuffer, position: Int, value: T)

  def maxIntermediateByteSize: Int

  def typeName: String = throw new UnsupportedOperationException(this + " does not support getTypeName()")
}

trait FloatRepresentation[T] { self : BufferCodec[T] =>

  def fromFloat(value: Float) : T

  def toFloat(value: T): Float

  override def typeName: String = "float"
}


object BufferCodec {

  implicit object IntCodec extends BufferCodec[Int] with FloatRepresentation[Int] {
    def read(buf: ByteBuffer, position: Int) = buf.getInt(position)

    def write(buf: ByteBuffer, position: Int, value: Int) {
      buf.putInt(position, value)
    }

    def fromFloat(f: Float) = f.toInt

    def toFloat(value: Int) = value.toFloat

    def maxIntermediateByteSize: Int = Ints.BYTES
  }

}
