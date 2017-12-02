package org.neo4j.cypher.internal.ir.v3_4

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection

class IRPrettyPrinterTest extends CypherFunSuite {
  test("minimal qg with only pattern nodes") {
    val qg = QueryGraph.empty.withPatternNodes(Set(IdName("a"), IdName("b"), IdName("c")))

    prettify(qg) should equal("QueryGraph { Nodes: ['a', 'b', 'c'] }")
  }

  test("minimal qg with only arguments") {
    val qg = QueryGraph.empty.withArgumentIds(Set(IdName("a"), IdName("b"), IdName("c")))

    prettify(qg) should equal("QueryGraph { Arguments: ['a', 'b', 'c'] }")
  }

  test("pattern nodes and relationships") {
    val qg = QueryGraph.empty.
      withPatternNodes(Set(IdName("a"), IdName("b"))).
      withPatternRelationships(Set(PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)))

    prettify(qg) should equal("QueryGraph { Nodes: ['a', 'b'] Edges: [('a')-['r']->('b')] }")
  }

//  test("pattern nodes and relationships") {
//    val qg = QueryGraph.empty.
//      withPatternNodes(Set(IdName("a"), IdName("b"))).
//      withPatternRelationships(Set(PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)))
//
//    prettify(qg) should equal("QueryGraph { Nodes: ['a', 'b'] Edges: [('a')-['r']->('b')] }")
//  }
//

  private def prettify(qg: QueryGraph): String = {
    val doc = IRPrettyPrinter.show(qg)
    for(i <- 0 until 70) {
      println(IRPrettyPrinter.pretty(doc, i).layout)
      println("*" * i)
    }
    IRPrettyPrinter.pretty(doc, 120).layout
  }
}
