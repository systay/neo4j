package org.neo4j.cypher.internal.compiler.v2_3.birk

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.graphdb.Direction

class CodeGeneratorTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val generator = new CodeGenerator

  test("all nodes scan") { // MATCH a RETURN a
    val plan = ProduceResult(List("a"), AllNodesScan(IdName("a"), Set.empty)(solved))

    println(generator.generate(plan))
  }

  test("all nodes scan + expand") { // MATCH (a)-[r]->(b) RETURN a, b
    val plan = ProduceResult(List("a", "b"),
      Expand(
        AllNodesScan(IdName("a"), Set.empty)(solved), IdName("a"), Direction.OUTGOING, Seq.empty, IdName("b"), IdName("r"), ExpandAll)(solved))


    println(generator.generate(plan))
  }

  test("hash join") {
    // MATCH (a)-[r1]->(b)<-[r2]-(c) RETURN b

    val lhs = Expand(AllNodesScan(IdName("a"), Set.empty)(solved), IdName("a"), Direction.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), ExpandAll)(solved)
    val rhs = Expand(AllNodesScan(IdName("c"), Set.empty)(solved), IdName("c"), Direction.INCOMING, Seq.empty, IdName("b"), IdName("r2"), ExpandAll)(solved)
    val join = NodeHashJoin(Set(IdName("b")), lhs, rhs)(solved)
    val plan = ProduceResult(List("b"), join)

    println(generator.generate(plan))
  }

  test("hash join double") {
    // MATCH (a)-[r1]->(b)<-[r2]-(c)<-[r3]-(d) RETURN b

    val lhs = Expand(AllNodesScan(IdName("a"), Set.empty)(solved), IdName("a"), Direction.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), ExpandAll)(solved)
    val rhs1 = Expand(AllNodesScan(IdName("c"), Set.empty)(solved), IdName("c"), Direction.INCOMING, Seq.empty, IdName("b"), IdName("r2"), ExpandAll)(solved)
    val rhs2 = Expand(AllNodesScan(IdName("d"), Set.empty)(solved), IdName("d"), Direction.INCOMING, Seq.empty, IdName("c"), IdName("r3"), ExpandAll)(solved)
    val join1 = NodeHashJoin(Set(IdName("b")), lhs, rhs1)(solved)
    val join2 = NodeHashJoin(Set(IdName("c")), join1, rhs2)(solved)
    val plan = ProduceResult(List("b"), join2)

    println(generator.generate(plan))
  }
}
