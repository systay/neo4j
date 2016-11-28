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

import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_2.{DummyPosition, InvalidSemanticsException}

class VariableBindingTest extends CypherFunSuite {
  val pos = DummyPosition(0)

  test("RETURN x should fail with missing variable") {
    val items = ReturnItems(includeExisting = false, Seq(UnaliasedReturnItem(Variable("x")(pos), "x")(pos)))(pos)
    val returnClause = Return(distinct = false, items, None, None, None)(pos)

    val singleQuery = SingleQuery(Seq(returnClause))(pos)
    val query = Query(None, singleQuery)(pos)

    Scoping.enrich(query)


    intercept[InvalidSemanticsException](VariableBinding.enrich(query))
  }

  test("FOREACH(x in x | CREATE ())") {
    val createPattern = Pattern(Seq(EveryPath(NodePattern(None, Seq.empty, None)(pos))))(pos)
    val create = Create(createPattern)(pos)

    val foreach = Foreach(Variable("x")(pos), Variable("x")(pos), Seq(create))(pos)

    val singleQuery = SingleQuery(Seq(foreach))(pos)
    val query = Query(None, singleQuery)(pos)

    Scoping.enrich(query)

    intercept[InvalidSemanticsException](VariableBinding.enrich(query))
  }

  test("MATCH (a) FOREACH(x in a.prop | CREATE ())") {
    val matchVariable = Variable("a")(pos)
    val matchPattern = Pattern(Seq(EveryPath(NodePattern(Some(matchVariable), Seq.empty, None)(pos))))(pos)
    val matchClause = Match(optional = false, matchPattern, Seq.empty, None)(pos)

    val createPattern = Pattern(Seq(EveryPath(NodePattern(None, Seq.empty, None)(pos))))(pos)
    val create = Create(createPattern)(pos)
    val foreachVariable = Variable("a")(pos)
    val foreach = Foreach(Variable("x")(pos), Property(foreachVariable, PropertyKeyName("prop")(pos))(pos), Seq(create))(pos)

    val singleQuery = SingleQuery(Seq(matchClause, foreach))(pos)
    val query = Query(None, singleQuery)(pos)

    Scoping.enrich(query)
    VariableBinding.enrich(query)

    matchVariable.binding.value should equal(Declaration)
    foreachVariable.binding.value should equal(Bound(matchVariable))
  }
}
