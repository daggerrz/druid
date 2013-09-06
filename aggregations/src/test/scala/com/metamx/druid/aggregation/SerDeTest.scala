package com.metamx.druid.aggregation

import org.junit.Test
import com.metamx.druid.indexer.data.{StringInputRowParser, TimestampSpec, JSONDataSpec}
import com.metamx.druid.index.v1._
import com.metamx.druid.{AllGranularity, DurationGranularity}
import com.google.common.io.Files
import com.metamx.druid.query.group.{GroupByQuery, GroupByQueryEngineConfig, GroupByQueryEngine}
import com.metamx.druid.collect.StupidPool
import java.nio.{ByteOrder, ByteBuffer}
import com.google.common.base.Supplier
import com.metamx.druid.query.segment.QuerySegmentSpecs
import org.joda.time.Interval
import com.metamx.druid.aggregation.post.PostAggregator
import com.metamx.druid.query.dimension.{DimensionSpec, DefaultDimensionSpec}
import com.metamx.druid.index.QueryableIndex
import com.metamx.druid.input.{MapBasedRow, Row}
import org.junit.Assert._
import com.metamx.common.guava.Accumulator
import com.metamx.druid.index.v1.serde.ComplexMetrics
import scala.collection.JavaConverters._
import com.twitter.algebird.HLL


class SerDeTest {


  val startTime = 0L
  val dimensions = List("gender", "age").asJava
  val dimensionExcludes = List.empty[String].asJava

  val data = new JSONDataSpec(dimensions, List.empty[SpatialDimensionSchema].asJava)
  val time = new TimestampSpec("utcdt", "iso")
  val granularity = new DurationGranularity(60 * 1000, startTime)
  val rowParser = new StringInputRowParser(time, data, dimensionExcludes)

  ComplexMetrics.registerSerde(Average.AverageCodec.typeName, Average.SerDe)
  ComplexMetrics.registerSerde(StringCardinality.Codec.typeName, StringCardinality.SerDe)

  def buildIndex(): IncrementalIndex = {
    val aggs = Array[AggregatorFactory](
      new CountAggregatorFactory("user_count"),
      new IntAverage(fieldName = "income", name = "income_average"),
      new StringCardinality(fieldName = "name", name = "unique_names")
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
    scala.io.Source.fromInputStream(in, "UTF-8").getLines().foreach {
      line =>
        index.add(rowParser.parse(line))
    }
    index
  }

  def doFullRoundTrip(dimensions: Seq[String], aggs: Array[AggregatorFactory]): com.metamx.common.guava.Sequence[Row] = {
    val index = buildIndex()
    val indexDir = Files.createTempDir()
    IndexMerger.persist(index, indexDir)

    val queryIndex: QueryableIndex = IndexIO.loadIndex(indexDir)

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
      aggs.toList.asJava,
      List.empty[PostAggregator].asJava,
      null, // havingSpec,
      null, // limitSpec,
      null, // orderbySpec
      null // context
    )
    val rows: com.metamx.common.guava.Sequence[Row] = queryEngine.process(query, new QueryableIndexStorageAdapter(queryIndex))
    rows
  }

  @Test def testAverageBufferAggregation() {

    val rows = doFullRoundTrip(Seq("age"), Array[AggregatorFactory](
      new CountAggregatorFactory("user_count"),
      new IntAverage(fieldName = "income_average", name = "income_average")
    ))

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

    println("RESULTS: " + results)
    assertEquals(results(100).avg, 75)
    assertEquals(results(50).avg, 150)

  }

  @Test def testCardinalityEstimator() {

    val rows = doFullRoundTrip(Nil, Array[AggregatorFactory](
      new StringCardinality(fieldName = "unique_names", name = "unique_names")
    ))

    val accumulator = new Accumulator[HLL, Row] {
      def accumulate(accumulated: HLL, in: Row): HLL = {
        val mapRow = in.asInstanceOf[MapBasedRow]
        val uniques = mapRow.getEvent.get("unique_names").asInstanceOf[HLL]
        StringCardinality.m.plus(accumulated, uniques)
      }
    }
    val results = rows.accumulate[HLL](StringCardinality.identity, accumulator)

    println("RESULTS: " + results.estimatedSize)

  }

  @Test def testAggregations() {
    val index = buildIndex()
    val results = index.iterator().asScala.map { row =>
      row.getDimension("user_count").get(0).toInt -> row.getDimension("income_average").get(0)
    }
    println(results.toList)
    assert(results.contains(2 -> 75))
    assert(results.contains(1 -> 200))
  }


}
