/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.codegen.ir.expressions

import org.neo4j.cypher.internal.compiler.v2_3.codegen.{Variable, CodeGenContext}
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class CodeGenExpressionTypesTest extends CypherFunSuite {

  val int = Literal(1: java.lang.Integer)
  val double = Literal(1.1: java.lang.Double)
  val string = Literal("apa")
  val node = NodeProjection(Variable("a", CTNode))
  val rel = RelationshipProjection(Variable("a", CTRelationship))
  val intCollection = Collection(Seq(int))
  val doubleCollection = Collection(Seq(double))
  val stringCollection = Collection(Seq(string))
  val nodeCollection = Collection(Seq(node))
  val relCollection = Collection(Seq(rel))

  test("collection") {
    implicit val context: CodeGenContext = null

    Collection(Seq(int)).cypherType should equal(CTCollection(CTInteger))
    Collection(Seq(double)).cypherType should equal(CTCollection(CTFloat))
    Collection(Seq(int, double)).cypherType should equal(CTCollection(CTNumber))
    Collection(Seq(string, int)).cypherType should equal(CTCollection(CTAny))
    Collection(Seq(node, rel)).cypherType should equal(CTCollection(CTMap))
  }

  test("add") {
    implicit val context: CodeGenContext = null

    Addition(int, double).cypherType should equal(CTFloat)
    Addition(string, int).cypherType should equal(CTAny)
    Addition(string, string).cypherType should equal(CTString)
    Addition(intCollection, int).cypherType should equal(CTCollection(CTInteger))
    Addition(int, intCollection).cypherType should equal(CTCollection(CTInteger))
    Addition(double, intCollection).cypherType should equal(CTCollection(CTNumber))
    Addition(doubleCollection, intCollection).cypherType should equal(CTCollection(CTNumber))
    Addition(stringCollection, string).cypherType should equal(CTCollection(CTString))
    Addition(string, stringCollection).cypherType should equal(CTCollection(CTString))
  }
}
