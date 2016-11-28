/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.frontend.v3_2.semantic.analysis

import org.neo4j.cypher.internal.frontend.v3_2.DummyPosition
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite

class ScopingTest extends CypherFunSuite {
  val pos = DummyPosition(0)

  test("MATCH (a) RETURN *") {
    val path = EveryPath(NodePattern(Some(Variable("a")(pos)), Seq.empty, None)(pos))
    val pattern = Pattern(Seq(path))(pos)
    val matchClause = Match(optional = false, pattern, Seq.empty, None)(pos)

    val items = ReturnItems(includeExisting = true, Seq.empty)(pos)
    val returnClause = Return(distinct = false, items, None, None, None)(pos)

    val singleQuery = SingleQuery(Seq(matchClause, returnClause))(pos)
    val query = Query(None, singleQuery)(pos)

    Scoping.enrich(query)

    query.myScope.value._1 should equal(Scope.empty)
    singleQuery.myScope.value._1 should equal(Scope.empty)
    returnClause.myScope.value._1 should equal(Scope.empty.enterScope())
  }

  test("MATCH (a) FOREACH(x in [1,2] | CREATE ())") {
    val matchPattern = Pattern(Seq(EveryPath(NodePattern(Some(Variable("a")(pos)), Seq.empty, None)(pos))))(pos)
    val matchClause = Match(optional = false, matchPattern, Seq.empty, None)(pos)

    val createPattern = Pattern(Seq(EveryPath(NodePattern(None, Seq.empty, None)(pos))))(pos)
    val create = Create(createPattern)(pos)

    val foreach = Foreach(Variable("x")(pos), ListLiteral(Seq(SignedDecimalIntegerLiteral("1")(pos)))(pos), Seq(create))(pos)

    val singleQuery = SingleQuery(Seq(matchClause, foreach))(pos)
    val query = Query(None, singleQuery)(pos)

    Scoping.enrich(query)

    query.myScope.value._1 should equal(Scope.empty)
    singleQuery.myScope.value._1 should equal(Scope.empty)
    val matchScope = Scope.empty.enterScope()
    foreach.myScope.value._1 should equal(matchScope)
    val createScope = matchScope.enterScope()
    create.myScope.value._1 should equal(createScope)
  }
}
