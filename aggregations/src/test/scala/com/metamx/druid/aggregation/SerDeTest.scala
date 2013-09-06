package com.metamx.druid.aggregation

import org.junit.Test
import com.metamx.druid.indexer.data.{StringInputRowParser, TimestampSpec, JSONDataSpec}
import com.metamx.druid.index.v1._
import com.metamx.druid.{AllGranularity, DurationGranularity}
import com.google.common.io.Files
import com.metamx.druid.query.group.{GroupByQuery, GroupByQueryEngineConfig, GroupByQueryEngine}
import com.metamx.druid.collect.StupidPool
import java.nio.ByteBuffer
import com.google.common.base.Supplier
import com.metamx.druid.query.segment.QuerySegmentSpecs
import org.joda.time.Interval
import com.metamx.druid.aggregation.post.PostAggregator
import com.metamx.druid.query.dimension.{DimensionSpec, DefaultDimensionSpec}
import com.metamx.druid.index.QueryableIndex
import com.metamx.druid.input.{MapBasedRow, MapBasedInputRow, Row}
import org.junit.Assert._
import com.metamx.common.guava.Accumulator
import com.metamx.druid.index.v1.serde.ComplexMetrics


class SerDeTest {

  import scala.collection.JavaConverters._

  val startTime = 0L
  val dimensions = List("gender", "age").asJava
  val dimensionExcludes = List.empty[String].asJava

  val data = new JSONDataSpec(dimensions, List.empty[SpatialDimensionSchema].asJava)
  val time = new TimestampSpec("utcdt", "iso")
  val granularity = new DurationGranularity(60 * 1000, startTime)
  val rowParser = new StringInputRowParser(time, data, dimensionExcludes)
  val aggs = Array[AggregatorFactory](
    new CountAggregatorFactory("user_count"),
    new IntAverage(name = "income_average", fieldName = "income")
  )

  def buildIndex(): IncrementalIndex = {
    val index = new IncrementalIndex(
      new IncrementalIndexSchema.Builder()
        .withMinTimestamp(startTime)
        .withSpatialDimensions(data.getSpatialDimensions)
        .withQueryGranularity(granularity)
        .withMetrics(aggs)
        .build()
    )

    val in = getClass.getResourceAsStream("/users.json")
    scala.io.Source.fromInputStream(in, "UTF-8").getLines().foreach {
      line =>
        index.add(rowParser.parse(line))
    }
    index
  }

  @Test def testAggregations() {
    val index = buildIndex()
    val results = index.iterator().asScala.map { row =>
        row.getDimension("user_count").get(0).toInt -> row.getDimension("income_average").get(0)
    }
    assert(results.contains(2 -> 75))
    assert(results.contains(1 -> 200))
  }

  @Test def testBufferAggregations() {

    ComplexMetrics.registerSerde(Average.AverageCodec.typeName, Average.SerDe)

    val index = buildIndex()
    val indexDir = Files.createTempDir()
    IndexMerger.persist(index, indexDir)

    val queryIndex : QueryableIndex = IndexIO.loadIndex(indexDir)


    val queryEngine: GroupByQueryEngine = {
      val config = new GroupByQueryEngineConfig {
        def getMaxIntermediateRows: Int = 5
      }
      val supplier = new Supplier[ByteBuffer]() {
        def get(): ByteBuffer = ByteBuffer.allocate(1024 * 1024)
      }
      val pool = new StupidPool[ByteBuffer](supplier)
      new GroupByQueryEngine(config, pool)
    }

    val query = new GroupByQuery(
      "example-source",
      QuerySegmentSpecs.create(new Interval(0, System.currentTimeMillis())),
      null,
      new AllGranularity,
      List[DimensionSpec](new DefaultDimensionSpec("age", "age")).asJava,
      aggs.toList.asJava,
      List.empty[PostAggregator].asJava,
      null, // havingSpec,
      null, // limitSpec,
      null, // orderbySpec
      null // context
    )
    val rows : com.metamx.common.guava.Sequence[Row] = queryEngine.process(query, new QueryableIndexStorageAdapter(queryIndex))


    type ByAge = Map[Int, Average]

    val accumulator = new Accumulator[ByAge, Row] {
      def accumulate(accumulated: ByAge, in: Row): ByAge = {
        val mapRow = in.asInstanceOf[MapBasedRow]
        val age = in.getFloatMetric("age").toInt
        val incomeAverage = mapRow.getEvent.get("income_average").asInstanceOf[Average]
        val existing = accumulated(age)
        accumulated + (age -> Average.apply(existing, incomeAverage))

      }
    }
    val results = rows.accumulate[ByAge](Map.empty[Int, Average].withDefault(x => Average.identity), accumulator)

    println(results)
    assertEquals(results(2), 75)
    assertEquals(results(1), 200)

  }
}
