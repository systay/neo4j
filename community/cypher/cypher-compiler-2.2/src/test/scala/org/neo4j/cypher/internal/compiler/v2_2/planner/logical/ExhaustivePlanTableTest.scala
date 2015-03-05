package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.LogicalPlan


class ExhaustivePlanTableTest extends CypherFunSuite {
  test("removeAll") {
    val planTable = new ExhaustivePlanTable()

    val s1 = mock[Solvable]
    val s2 = mock[Solvable]
    val s3 = mock[Solvable]

    planTable.put(Set(s1), mock[LogicalPlan])
    planTable.put(Set(s2), mock[LogicalPlan])
    planTable.put(Set(s3), mock[LogicalPlan])
    planTable.put(Set(s1, s2), mock[LogicalPlan])
    planTable.put(Set(s2, s3), mock[LogicalPlan])
    planTable.put(Set(s1, s3), mock[LogicalPlan])

    planTable.removeAllTracesOf(Set(s1,s2))

    planTable.keySet should equal(Set(
      Set(s3)
    ))
  }
}
