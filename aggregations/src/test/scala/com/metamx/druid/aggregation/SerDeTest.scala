package com.metamx.druid.aggregation

import org.junit.Test
import com.metamx.druid.indexer.data.{StringInputRowParser, TimestampSpec, JSONDataSpec}
import com.metamx.druid.index.v1.{IncrementalIndexSchema, IncrementalIndex, SpatialDimensionSchema}
import com.metamx.druid.{DurationGranularity, BaseQueryGranularity}
import org.joda.time.DateTime

class SerDeTest {
  import scala.collection.JavaConverters._

  val startTime = 0L
  val dimensions = List("gender", "age").asJava
  val dimensionExcludes = List.empty[String].asJava

  @Test def testAggregations() {
    val data = new JSONDataSpec(dimensions, List.empty[SpatialDimensionSchema].asJava)
    val time = new TimestampSpec("utcdt", "iso")
    val granularity = new DurationGranularity(60 * 1000, startTime)
    val rowParser = new StringInputRowParser(time, data, dimensionExcludes)
    val aggs = Array[AggregatorFactory](
      new CountAggregatorFactory("user_count"),
      new IntAverage(name = "income_average", fieldName = "income")
    )

    val index = new IncrementalIndex(
      new IncrementalIndexSchema.Builder()
        .withMinTimestamp(startTime)
        .withSpatialDimensions(data.getSpatialDimensions)
        .withQueryGranularity(granularity)
        .withMetrics(aggs)
        .build()
    )

    val in = getClass.getResourceAsStream("/users.json")
    scala.io.Source.fromInputStream(in, "UTF-8").getLines().foreach { line =>
      index.add(rowParser.parse(line))
    }

    index.iterator().asScala.foreach { row =>
      println(row.getDimension("user_count"))
    }
  }
}
