/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
///**
// * Copyright (c) 2002-2012 "Neo Technology,"
// * Network Engine for Objects in Lund AB [http://neotechnology.com]
// *
// * This file is part of Neo4j.
// *
// * Neo4j is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program.  If not, see <http://www.gnu.org/licenses/>.
// */
//package org.neo4j.cypher.internal.executionplan.builders
//
//import org.junit.Test
//import org.neo4j.cypher.internal.executionplan.PartiallySolvedQuery
//import org.junit.Assert._
//import org.neo4j.cypher.internal.commands._
//
//class SortedAggregationBuilderTest extends BuilderTest {
//
//  val builder = new SortedAggregationBuilder
//
//  @Test def should_accept_when_aggregating_on_same_keys_as_sort() {
//    val q = PartiallySolvedQuery().
//      copy(aggregation = Seq(Unsolved(CountStar())),
//      returns = Seq(Unsolved(ReturnItem(Entity("n"), "n"))),
//      sort = Seq(Unsolved(SortItem(Entity("n"), true)))
//    )
//
//    val p = createPipe(nodes = Seq("n"))
//
//    assertTrue("Builder should accept this", builder.canWorkWith(plan(p, q)))
//
//    val resultQ = builder(plan(p, q)).query
//
//    val expectedQuery = q.copy(
//      aggregation = q.aggregation.map(_.solve),
//      sort = q.sort.map(_.solve),
//      extracted = true
//    )
//
//    assert(resultQ == expectedQuery)
//  }
//
//  @Test def should_not_accept_if_the_sorting_columns_are_in_a_different_order() {
//    val q = PartiallySolvedQuery().
//      copy(aggregation = Seq(Unsolved(CountStar())),
//      returns = Seq(Unsolved(ReturnItem(Entity("a"), "a")), Unsolved(ReturnItem(Entity("b"), "b"))),
//      sort = Seq(Unsolved(SortItem(Entity("b"), true)), Unsolved(SortItem(Entity("a"), true)))
//    )
//
//    val p = createPipe(nodes = Seq("a", "b"))
//
//    assertFalse("Builder should not accept this", builder.canWorkWith(plan(p, q)))
//  }
//
//  @Test def should_not_accept_if_there_are_still_other_things_to_do_in_the_query() {
//    val q = PartiallySolvedQuery().
//      copy(
//      start = Seq(Unsolved(NodeById("n", 0))),
//      aggregation = Seq(Unsolved(CountStar())),
//      returns = Seq(Unsolved(ReturnItem(Entity("n"), "n"))),
//      sort = Seq(Unsolved(SortItem(Entity("n"), true)))
//    )
//
//    val p = createPipe(nodes = Seq())
//
//    assertFalse("Builder should not accept this", builder.canWorkWith(plan(p, q)))
//  }
//}