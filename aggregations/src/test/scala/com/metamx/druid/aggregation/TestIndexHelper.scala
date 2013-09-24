package com.metamx.druid.aggregation

import com.metamx.druid.index.v1._
import com.metamx.druid.{AllGranularity, DurationGranularity}
import com.metamx.druid.indexer.data.{StringInputRowParser, TimestampSpec, JSONDataSpec}
import scala.collection.JavaConverters._
import com.metamx.druid.index.{Segment, QueryableIndex}
import com.google.common.io.Files
import com.metamx.druid.input.Row
import com.metamx.druid.query.group.{GroupByQuery, GroupByQueryEngineConfig, GroupByQueryEngine}
import com.google.common.base.Supplier
import java.nio.{ByteOrder, ByteBuffer}
import com.metamx.druid.collect.StupidPool
import com.metamx.druid.query.segment.QuerySegmentSpecs
import org.joda.time.Interval
import com.metamx.druid.query.dimension.{DimensionSpec, DefaultDimensionSpec}
import com.metamx.druid.aggregation.post.PostAggregator

object TestIndexHelper {
  val startTime = 0L
  val granularity = new DurationGranularity(60 * 1000, startTime)
  val dimensions = List("gender", "age").asJava
  val dimensionExcludes = List.empty[String].asJava
  val data = new JSONDataSpec(dimensions, List.empty[SpatialDimensionSchema].asJava)

  val time = new TimestampSpec("utcdt", "iso")
  val rowParser = new StringInputRowParser(time, data, dimensionExcludes)

  /**
   * Build an incremental index and add example data from users.json to it.
   */
  def buildIncrementalIndex(aggregations: Seq[AggregatorFactory]) : IncrementalIndex = {
    val index = new IncrementalIndex(
      new IncrementalIndexSchema.Builder()
        .withMinTimestamp(startTime)
        .withSpatialDimensions(data.getSpatialDimensions)
        .withQueryGranularity(granularity)
        .withMetrics(aggregations.toArray)
        .build()
    )

    val in = getClass.getResourceAsStream("/users.json")
    scala.io.Source.fromInputStream(in, "UTF-8").getLines().foreach { line => index.add(rowParser.parse(line)) }
    index
  }

  /**
   * Persist the index and load it back in. Ensures that the on-disk format works.
   */
  def persistAndLoadIndex(index: IncrementalIndex) : QueryableIndex = {
    val indexDir = Files.createTempDir()
    IndexMerger.persist(index, indexDir)
    IndexIO.loadIndex(indexDir)
  }

  /**
   * Executes a GroupBy query given segment and returns the raw rows.
   */
  def querySegment(segment: Segment, dimensions: Seq[String], aggregators: Seq[AggregatorFactory]): com.metamx.common.guava.Sequence[Row] = {

    val queryEngine: GroupByQueryEngine = {
      val config = new GroupByQueryEngineConfig {
        def getMaxIntermediateRows: Int = 5
      }
      val supplier = new Supplier[ByteBuffer]() {
        def get(): ByteBuffer = ByteBuffer.allocateDirect(0xFFFF).order(ByteOrder.LITTLE_ENDIAN)

      }
      val pool = new StupidPool[ByteBuffer](supplier)
      new GroupByQueryEngine(config, pool)
    }

    val query = new GroupByQuery(
      "example-source",
      QuerySegmentSpecs.create(new Interval(0, System.currentTimeMillis())),
      null,
      new AllGranularity,
      dimensions.map(n => new DefaultDimensionSpec(n, n)).toList.asInstanceOf[List[DimensionSpec]].asJava,
      aggregators.toList.asJava,
      List.empty[PostAggregator].asJava,
      null, // havingSpec,
      null, // limitSpec,
      null, // orderbySpec
      null // context
    )
    val rows: com.metamx.common.guava.Sequence[Row] = queryEngine.process(query, segment.asStorageAdapter())
    rows
  }

}
