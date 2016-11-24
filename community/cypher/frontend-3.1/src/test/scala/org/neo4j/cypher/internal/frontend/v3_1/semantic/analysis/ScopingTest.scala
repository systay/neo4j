package org.neo4j.cypher.internal.frontend.v3_1.semantic.analysis

import org.neo4j.cypher.internal.frontend.v3_1.DummyPosition
import org.neo4j.cypher.internal.frontend.v3_1.ast._
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite

class ScopingTest extends CypherFunSuite {
  val pos = DummyPosition(0)

  test("MATCH (a) WITH a RETURN *") {
    val path = EveryPath(NodePattern(None, Seq.empty, None)(pos))
    val pattern = Pattern(Seq(path))(pos)
    val matchClause = Match(optional = false, pattern, Seq.empty, None)(pos)

    val items = ReturnItems(includeExisting = true, Seq.empty)(pos)
    val returnClause = Return(distinct = false, items, None, None, None)(pos)

    val singleQuery = SingleQuery(Seq(matchClause, returnClause))(pos)
    val query = Query(None, singleQuery)(pos)

    Scoping.enrich(query)

    query.myScope.value should equal(MyScope.empty)
    singleQuery.myScope.value should equal(MyScope.empty.enterScope())
    returnClause.myScope.value should equal(MyScope.empty.enterScope())
  }
}
