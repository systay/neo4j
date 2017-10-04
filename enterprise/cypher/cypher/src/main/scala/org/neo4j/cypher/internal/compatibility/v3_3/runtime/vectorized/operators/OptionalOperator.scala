package org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized.{Iteration, MiddleOperator, Morsel, QueryState}
import org.neo4j.cypher.internal.spi.v3_3.QueryContext

class OptionalOperator(nullableOffsets: Set[Int]) extends MiddleOperator {
  override def operate(iterationState: Iteration, data: Morsel, context: QueryContext, state: QueryState): Unit = {
    if(data.validRows == 0) {
      nullableOffsets.foreach { idx =>
        data.longs(idx) = -1
      }
      data.validRows = 1
    }
  }
}
