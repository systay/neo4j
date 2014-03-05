/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v2_1.planner.{Selections, SimpleQueryGraphBuilder}
import org.neo4j.cypher.internal.compiler.v2_1.{InputPosition, DummyPosition}
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.parser.CypherParser
import org.neo4j.cypher.internal.compiler.v2_1.DummyPosition
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.Id

class SimpleQueryGraphBuilderTest extends CypherFunSuite {

  val parser = new CypherParser()
  val pos = DummyPosition(0)

  test("projection only query") {
    val ast = parse("RETURN 42")
    val builder = new SimpleQueryGraphBuilder
    val qg = builder.produce(ast)
    qg.projections should equal(Map("42" -> SignedIntegerLiteral("42")(pos)))
  }

  test("multiple projection query") {
    val ast = parse("RETURN 42, 'foo'")
    val builder = new SimpleQueryGraphBuilder
    val qg = builder.produce(ast)
    qg.projections should equal(Map(
      "42" -> SignedIntegerLiteral("42")(pos),
      "'foo'" -> StringLiteral("foo")(pos)
    ))
  }

  test("match n return n") {
    val ast = parse("MATCH n RETURN n")
    val builder = new SimpleQueryGraphBuilder
    val qg = builder.produce(ast)

    qg.projections should equal(Map(
      "n" -> Identifier("n")(pos)
    ))

    qg.identifiers should equal(Set(Id("n")))
  }

  test("match n where n:Awesome return n") {
    val ast = parse("MATCH n WHERE n:Awesome:Foo RETURN n")
    val builder = new SimpleQueryGraphBuilder
    val qg = builder.produce(ast)

    qg.projections should equal(Map(
      "n" -> Identifier("n")(pos)
    ))

    qg.selections should equal(Selections(Seq(
      Set(Id("n")) -> HasLabels(Identifier("n")(pos), Seq(LabelName("Awesome")()(pos)))(pos),
      Set(Id("n")) -> HasLabels(Identifier("n")(pos), Seq(LabelName("Foo")()(pos)))(pos)
    )))

    qg.identifiers should equal(Set(Id("n")))
  }

  def parse(s: String): Query =
    parser.parse(s).asInstanceOf[Query]
}
