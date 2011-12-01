package org.neo4j.cypher.pipes

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
import org.scalatest.Assertions
import org.neo4j.cypher.GraphDatabaseTestBase
import org.neo4j.cypher.commands.ShortestPath
import org.neo4j.graphdb.{Direction, Node, Path}
import collection.Traversable

class AllShortestPathsPipeTest extends GraphDatabaseTestBase with Assertions {
  def runThroughPipeAndGetPath(a: Node, b: Node): Traversable[Path] = {
    val source = new FakePipe(List(Map("a" -> a, "b" -> b)))

    val pipe = new AllShortestPathsPipe(source, ShortestPath("p", "a", "b", None, Direction.BOTH, Some(15), true, false))
    pipe.map(m => m("p").asInstanceOf[Path])
  }

  @Test def shouldReturnTheShortestPathBetweenTwoNodes() {
    val (a,b,c,d) = createDiamond()

    val resultPaths = runThroughPipeAndGetPath(a, d)

    assert(resultPaths.size === 2)

    resultPaths.foreach(resultPath => {
      val number_of_relationships_in_path = resultPath.length()

      assert(number_of_relationships_in_path === 2)
      assert(resultPath.startNode() === a)
      assert(resultPath.endNode() === d)
    })
  }

  @Test def shouldReturnNullWhenOptional() {
    val a = createNode("a")
    val b = createNode("b")
    // The secret is in what's not there - there is no relationship between a and b

    assert(runThroughPipeAndGetPath(a, b) === List(null))
  }
}