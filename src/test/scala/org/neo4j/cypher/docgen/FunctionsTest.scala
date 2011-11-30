package org.neo4j.cypher.docgen

/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.junit.Test
import org.junit.Assert._
import org.neo4j.graphdb.Node
import org.neo4j.cypher.ExecutionResult

class FunctionsTest extends DocumentingTestBase {
  def graphDescription = List("A KNOWS B", "A KNOWS C", "B KNOWS D", "C KNOWS D", "B MARRIED E")

  override val properties = Map(
    "A" -> Map("age" -> 38, "eyes" -> "brown"),
    "B" -> Map("age" -> 25, "eyes" -> "blue"),
    "C" -> Map("age" -> 53, "eyes" -> "green"),
    "D" -> Map("age" -> 54, "eyes" -> "brown"),
    "E" -> Map("age" -> 41, "eyes" -> "blue")
  )


  def section = "functions"

  val common_arguments = List(
    "iterable" -> "An array property, or an iterable symbol, or an iterable function.",
    "symbol" -> "This is the identifier that can be used from the predicate.",
    "predicate" -> "A predicate that is tested against all items in iterable"
  )

  @Test def all() {
    testThis(
      title = "ALL",
      syntax = "ALL(identifier in iterable WHERE predicate)",
      arguments = common_arguments,
      text = """Tests whether a predicate holds for all element of this iterable collection.""",
      queryText = """start a=node(%A%), b=node(%D%) match p=a-[*1..3]->b where all(x in nodes(p) WHERE x.age > 30) return p""",
      returns = """All nodes in the path.""",
      (p) => assertEquals(1, p.toSeq.length))
  }

  @Test def any() {
    testThis(
      title = "ANY",
      syntax = "ANY(identifier in iterable WHERE predicate)",
      arguments = common_arguments,
      text = """Tests whether a predicate holds for at least one element of this iterable collection.""",
      queryText = """start a=node(%A%) match p=a-[*1..3]->b where any(x in nodes(p) WHERE x.eyes = "blue") return p""",
      returns = """All nodes in the path.""",
      (p) => assertEquals(3, p.toSeq.length))
  }

  @Test def none() {
    testThis(
      title = "NONE",
      syntax = "NONE(identifier in iterable WHERE predicate)",
      arguments = common_arguments,
      text = """Returns true if the predicate holds for no element in the iterable.""",
      queryText = """start n=node(%A%) match p=n-[*1..3]->b where NONE(x in nodes(p) WHERE x.age = 25) return p""",
      returns = """All nodes in the path.""",
      (p) => assertEquals(2, p.toSeq.length))
  }

  @Test def single() {
    testThis(
      title = "SINGLE",
      syntax = "SINGLE(identifier in iterable WHERE predicate)",
      arguments = common_arguments,
      text = """Returns true if the predicate holds for exactly one of the elements in the iterable.""",
      queryText = """start n=node(%A%) match p=n-->b where SINGLE(var in nodes(p) WHERE var.eyes = "blue") return p""",
      returns = """All nodes in the path.""",
      (p) => assertEquals(1, p.toSeq.length))
  }

  @Test def relationship_type() {
    testThis(
      title = "TYPE",
      syntax = "TYPE( relationship )",
      arguments = List("relationship" -> "A relationship"),
      text = """Returns a string representation of the relationship type.""",
      queryText = """start n=node(%A%) match (n)-[r]->() return type(r)""",
      returns = """The relationship type of r.""",
      (p) => assertEquals("KNOWS", p.columnAs[String]("TYPE(r)").toList.head))
  }

  @Test def length() {
    testThis(
      title = "LENGTH",
      syntax = "LENGTH( iterable )",
      arguments = List("iterable" -> "An iterable, value or function call"),
      text = """To return or filter on the length of a path, use the special property LENGTH""",
      queryText = """start a=node(%A%) match p=a-->b-->c return length(p)""",
      returns = """The length of the path p.""",
      (p) => assertEquals(2, p.columnAs[Int]("LENGTH(p)").toList.head))
  }

  @Test def extract() {
    testThis(
      title = "EXTRACT",
      syntax = "EXTRACT( identifier in iterable : expression )",
      arguments = List(
        "iterable" -> "An array property, or an iterable symbol, or an iterable function.",
        "symbol" -> "The closure will have a symbol introduced in it's context. Here you decide which symbol to use.",
        "expression" -> "This expression will run once per value in the iterable, and produces the result iterable."
      ),
      text = """To return a single property, or the value of a function from an iterable of nodes or relationships,
 you can use EXTRACT. It will go through all enitities in the iterable, and run an expression, and return the results
 in an iterable with these values. It works like the `map` method in functional languages such as Lisp and Scala.""",
      queryText = """start a=node(%A%), b=node(%B%), c=node(%D%) match p=a-->b-->c return extract(n in nodes(p) : n.age)""",
      returns = """The age property of all nodes in the path.""",
      (p) => assertEquals(List(Map("extract(n in NODES(p) : n.age)" -> List(38, 25, 54))), p.toList))
  }

  @Test def nodes_in_path() {
    testThis(
      title = "NODES",
      syntax = "NODES( path )",
      arguments = List("path" -> "A path"),
      text = """Returns all nodes in a path""",
      queryText = """start a=node(%A%), c=node(%E%) match p=a-->b-->c return NODES(p)""",
      returns = """All the nodes in the path p.""",
      (p) => assert(List(node("A"), node("B"), node("E")) === p.columnAs[List[Node]]("NODES(p)").toList.head)
    )
  }

  @Test def rels_in_path() {
    testThis(
      title = "RELATIONSHIPS",
      syntax = "RELATIONSHIPS( path )",
      arguments = List("path" -> "A path"),
      text = """Returns all relationships in a path""",
      queryText = """start a=node(%A%), c=node(%E%) match p=a-->b-->c return RELATIONSHIPS(p)""",
      returns = """All the nodes in the path p.""",
      (p) => assert(2 === p.columnAs[List[Node]]("RELATIONSHIPS(p)").toSeq.head.length)
    )
  }

  @Test def id() {
    testThis(
      title = "ID",
      syntax = "ID( property-container )",
      arguments = List("property-container" -> "A node or a relationship"),
      text = """Returns the id of the relationship or node""",
      queryText = """start a=node(%A%, %B%, %C%) return ID(a)""",
      returns = """The node id for three nodes.""",
      (p) => assert(Seq(node("A").getId, node("B").getId, node("C").getId) === p.columnAs[Int]("ID(a)").toSeq)
    )
  }

  @Test def coalesce() {
    testThis(
      title = "COALESCE",
      syntax = "COALESCE( expression [, expression]* )",
      arguments = List("expression" -> "The expression that might return null"),
      text = """Returns the first non-null value in the list of expressions passed to it.""",
      queryText = """start a=node(%A%) return coalesce(a.hairColour?, a.eyes?)""",
      returns = """""",
      (p) => assert(Seq("brown") === p.columnAs[String]("COALESCE(a.hairColour,a.eyes)").toSeq)
    )
  }

  private def testThis(title: String, syntax: String, arguments: List[(String, String)], text: String, queryText: String, returns: String, assertions: (ExecutionResult => Unit)*) {
    val argsText = arguments.map(x => "* _" + x._1 + ":_ " + x._2).mkString("\r\n\r\n")
    val fullText = String.format("""%s

*Syntax:* `%s`

*Arguments:*

%s""", text, syntax, argsText)
    testQuery(title, fullText, queryText, returns, assertions: _*)
  }
}