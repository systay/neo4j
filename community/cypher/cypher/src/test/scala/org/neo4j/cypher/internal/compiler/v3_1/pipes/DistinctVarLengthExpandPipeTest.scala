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
package org.neo4j.cypher.internal.compiler.v3_1.pipes

import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.compiler.v3_1.QueryStateHelper.queryStateFrom
import org.neo4j.cypher.internal.frontend.v3_1.SemanticDirection

class DistinctVarLengthExpandPipeTest extends GraphDatabaseFunSuite {
  val types = LazyTypes(Seq.empty[String])
  implicit val pipeMonitor = mock[PipeMonitor]

  test("node without any relationships produces empty result") {
    val n1 = createNode()
    val src = new FakePipe(Iterator(Map("from" -> n1)))
    val pipeUnderTest = createPipe(src)

    graph.withTx { tx =>
      val queryState = queryStateFrom(graph, tx, Map.empty)
      pipeUnderTest.createResults(queryState) shouldBe empty
    }
  }

  test("node with a single relationships produces a single output node") {
    val n1 = createNode()
    val n2 = createNode()
    relate(n1, n2)
    val src = new FakePipe(Iterator(Map("from" -> n1)))
    val pipeUnderTest = createPipe(src)

    graph.withTx { tx =>
      val queryState = queryStateFrom(graph, tx, Map.empty)
      pipeUnderTest.createResults(queryState).toList shouldBe List(Map())
    }
  }

  def createPipe(src: FakePipe): DistinctVarLengthExpandPipe = {
    DistinctVarLengthExpandPipe(src, "from", "to", types, SemanticDirection.OUTGOING, 1, 2)()
  }
}
