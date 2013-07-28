package com.metamx.druid.aggregation

import org.junit.Test
import java.nio.ByteBuffer
import com.google.common.primitives.Ints

class BufferCodecTest {

  class TestCodec extends BufferCodec[Int] with FloatRepresentation[Int] {


    def fromFloat(value: Float) = value.toInt

    def toFloat(value: Int) = value.toFloat

    def read(buf: ByteBuffer, position: Int) = buf.getInt(position)

    def write(buf: ByteBuffer, position: Int, value: Int) { buf.putInt(position, value)}

    final def byteSize = Ints.BYTES
  }

  @Test def testFloatRepresentation() {

  }
}
