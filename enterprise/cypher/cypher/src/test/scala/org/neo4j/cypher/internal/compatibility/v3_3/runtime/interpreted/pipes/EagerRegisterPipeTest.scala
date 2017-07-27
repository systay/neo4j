/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.pipes

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, LongSlot, PipelineInformation}
import org.neo4j.cypher.internal.compiler.v3_3.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite

class EagerRegisterPipeTest extends CypherFunSuite {
  test("empty input gives empty output") {
    val source = mock[Pipe]
    when(source.createResults(any[QueryState]())).thenReturn(Iterator.empty)
    val testObject = EagerRegisterPipe(source, new PipelineInformation(Map.empty, 0, 0))()

    val result = testObject.createResults(QueryStateHelper.empty)

    result shouldBe empty
  }

  test("one element in, one element out") {
    val source = mock[Pipe]
    val pipeline = new PipelineInformation(Map.empty, 0, 0).
      add("x", LongSlot(0, nullable = false, CTNode, "x"))
    val ctx = PrimitiveExecutionContext(pipeline)
    ctx.setLongAt(0, 42)
    when(source.createResults(any[QueryState]())).thenReturn(Iterator(ctx))

    val testObject = EagerRegisterPipe(source, pipeline)()

    val result = testObject.createResults(QueryStateHelper.empty)
    expectedOutput(result, 42)
  }

  test("hundred in, hundred out") {
    val source = mock[Pipe]
    val pipeline = new PipelineInformation(Map.empty, 0, 0).
      add("x", LongSlot(0, nullable = false, CTNode, "x"))

    val values = (0 until 100) map (i => i + 42l)

    val input = values map { i =>
      val ctx = PrimitiveExecutionContext(pipeline)
      ctx.setLongAt(0, i)
      ctx
    }

    when(source.createResults(any[QueryState]())).thenReturn(input.iterator)

    val testObject = EagerRegisterPipe(source, pipeline)()

    val result = testObject.createResults(QueryStateHelper.empty)
    expectedOutput(result, values:_*)
  }

  private def expectedOutput(iter: Iterator[ExecutionContext], longs: Long*): Unit = {
    val expectation = longs.iterator

    while(iter.nonEmpty && expectation.nonEmpty) {
      val currentRow = iter.next()
      val currentValue = expectation.next()
      currentRow.getLongAt(0) should equal(currentValue)
    }

    iter shouldBe empty
    expectation shouldBe empty
  }
}
