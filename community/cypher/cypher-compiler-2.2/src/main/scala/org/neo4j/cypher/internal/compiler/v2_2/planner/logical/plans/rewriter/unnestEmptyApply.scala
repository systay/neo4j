package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{Apply, SingleRow}

case object unnestEmptyApply extends Rewriter {

  private val instance: Rewriter = Rewriter.lift {
    case Apply(sr: SingleRow, rhs) if sr.coveredIds.isEmpty => rhs
  }

  override def apply(input: AnyRef) = bottomUp(instance).apply(input)
}
