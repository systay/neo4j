package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._

class UnnestEmptyApplyTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("should unnest apply with a single SingleRow on the lhs") {
    val rhs = newMockedLogicalPlan()
    val input = Apply(SingleRow(Set.empty)(solved)(), rhs)(solved)

    input.endoRewrite(unnestEmptyApply) should equal(rhs)
  }

  test("should not take on plans that do not match the description") {
    val input = newMockedLogicalPlan()

    input.endoRewrite(unnestEmptyApply) should equal(input)
  }
}
