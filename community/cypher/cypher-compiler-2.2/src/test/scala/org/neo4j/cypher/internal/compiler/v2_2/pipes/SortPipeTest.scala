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
package org.neo4j.cypher.internal.compiler.v2_2.pipes

import org.neo4j.cypher.internal.compiler.v2_2.ExecutionContext
import org.scalatest.mock.MockitoSugar
import org.junit.Assert._
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import collection.mutable.{Map=>MutableMap}
import org.neo4j.cypher.internal.commons.CypherFunSuite

class SortPipeTest extends CypherFunSuite with MockitoSugar {

  private implicit val monitor = mock[PipeMonitor]

  test("empty input gives empty output") {
    val source = new FakePipe(List(), "x" -> CTAny)
    val sortPipe = new SortPipe(source, List(Ascending("x")))()

    assertEquals(List(), sortPipe.createResults(QueryStateHelper.empty).toList)
  }

  test("simple sorting is supported") {
    val list:Seq[MutableMap[String, Any]] = List(MutableMap("x" -> "B"), MutableMap("x" -> "A"))
    val source = new FakePipe(list, "x" -> CTString)
    val sortPipe = new SortPipe(source, List(Ascending("x")))()

    assertEquals(
      List(ExecutionContext("x" -> "A"), ExecutionContext("x" -> "B")),
      sortPipe.createResults(QueryStateHelper.empty).toList)
  }

  test("sort by two columns") {
    val source = new FakePipe(List(
      MutableMap("x" -> "B", "y" -> 20),
      MutableMap("x" -> "A", "y" -> 100),
      MutableMap("x" -> "B", "y" -> 10)), "x" -> CTString, "y"->CTNumber)

    val sortPipe = new SortPipe(source, List(
      Ascending("x"),
      Ascending("y")))()

    assertEquals(List(
      ExecutionContext("x" -> "A", "y" -> 100),
      ExecutionContext("x" -> "B", "y" -> 10),
      ExecutionContext("x" -> "B", "y" -> 20)), sortPipe.createResults(QueryStateHelper.empty).toList)
  }

  test("sort by two columns with one descending") {
    val source = new FakePipe(List(
      MutableMap("x" -> "B", "y" -> 20),
      MutableMap("x" -> "A", "y" -> 100),
      MutableMap("x" -> "B", "y" -> 10)), "x" -> CTString, "y"->CTNumber)

    val sortPipe = new SortPipe(source, List(
      Ascending("x"),
      Descending("y")))()

    assertEquals(List(
      ExecutionContext("x" -> "A", "y" -> 100),
      ExecutionContext("x" -> "B", "y" -> 20),
      ExecutionContext("x" -> "B", "y" -> 10)), sortPipe.createResults(QueryStateHelper.empty).toList)
  }

  test("should handle null values") {
    val list: Seq[MutableMap[String, Any]] = List(
      MutableMap("y" -> 1),
      MutableMap("y" -> null),
      MutableMap("y" -> 2))
    val source = new FakePipe(list, "y"->CTNumber)

    val sortPipe = new SortPipe(source, List(Ascending("y")))()

    assertEquals(List(
      ExecutionContext("y" -> 1),
      ExecutionContext("y" -> 2),
      ExecutionContext("y" -> null)), sortPipe.createResults(QueryStateHelper.empty).toList)
  }
}
