package com.metamx.druid.aggregation

import com.metamx.druid.index.v1.serde.ComplexMetrics
import com.metamx.common.guava.Accumulator
import com.twitter.algebird.HLL
import com.metamx.druid.input.{MapBasedRow, Row}
import org.junit.Test
import com.metamx.druid.index.{QueryableIndexSegment, IncrementalIndexSegment}
import org.junit.Assert._

class CardinalityEstimatorTest {
  import TestIndexHelper._

  ComplexMetrics.registerSerde(CardinalityAggregator.String.Codec.typeName, CardinalityAggregator.String.SerDe)

  val indexAggregations : Seq[AggregatorFactory] = Array(
    new StringCardinalityAggregator(fieldName = "name", name = "unique_names")
  )

  val computeAggregations : Seq[AggregatorFactory] = Array(
    new StringCardinalityAggregator(fieldName = "unique_names", name = "unique_names")
  )

  val nameAccumulator = new Accumulator[HLL, Row] {
    def accumulate(accumulated: HLL, in: Row): HLL = {
      val mapRow = in.asInstanceOf[MapBasedRow]
      val uniques = mapRow.getEvent.get("unique_names").asInstanceOf[HLL]
      CardinalityAggregator.String.m.plus(accumulated, uniques)
    }
  }

  /**
   * Tests the cardinality estimator's on-the-fly capabilities as a Aggregator.
   */
  @Test def testCardinalityComputation() {
    val segment = new IncrementalIndexSegment(buildIncrementalIndex(indexAggregations))
    val rows = querySegment(
      segment,
      dimensions = Nil,
      aggregators = computeAggregations
    )
    val results = rows.accumulate[HLL](CardinalityAggregator.String.identity, nameAccumulator)
    // John, Lisa, Peter,
    assertEquals(results.estimatedSize, 3.0, 0.1)
  }

  /**
   * Tests the cardinality estimator's BufferAggregator capabilities by adding it to a
   * persisted segment file.
   */
  @Test def testCardinalityEstimator() {

    val rows = querySegment(
      segment = new QueryableIndexSegment("segmentId", persistAndLoadIndex(buildIncrementalIndex(indexAggregations))),
      dimensions = Nil,
      aggregators = computeAggregations
    )
    val results = rows.accumulate[HLL](CardinalityAggregator.String.identity, nameAccumulator)
    // John, Lisa, Peter,
    assertEquals(results.estimatedSize, 3.0, 0.1)
  }

}
