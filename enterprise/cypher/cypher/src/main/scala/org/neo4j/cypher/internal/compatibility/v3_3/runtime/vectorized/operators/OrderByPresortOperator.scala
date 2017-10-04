package org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.Slot
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes.ColumnOrder
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized.{Iteration, MiddleOperator, Morsel, QueryState}
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.values.{AnyValue, AnyValues}

class OrderByPresortOperator(orderBy: Seq[ColumnOrder]) extends MiddleOperator {
  override def operate(iterationState: Iteration, output: Morsel, context: QueryContext, state: QueryState): Unit = {
//    java.util.Arrays.sort(array, comparator)
  }

  sealed trait ColumnOrder {
    def slot: Slot

    def compareValues(a: AnyValue, b: AnyValue): Int
    def compareLongs(a: Long, b: Long): Int
  }

  case class Ascending(slot: Slot) extends ColumnOrder {
    override def compareValues(a: AnyValue, b: AnyValue): Int = AnyValues.COMPARATOR.compare(a, b)
    override def compareLongs(a: Long, b: Long): Int = java.lang.Long.compare(a, b)
  }

  case class Descending(slot: Slot) extends ColumnOrder {
    override def compareValues(a: AnyValue, b: AnyValue): Int = AnyValues.COMPARATOR.compare(b, a)
    override def compareLongs(a: Long, b: Long): Int = java.lang.Long.compare(b, a)
  }

}

trait SortableMorsel {
  self : Morsel =>


}
