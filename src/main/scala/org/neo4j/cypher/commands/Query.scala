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
package org.neo4j.cypher.commands

import scala.Some
import org.neo4j.cypher.pipes.SortItem

object Query {
  def apply(returns:Return, start:Start) = new Query(returns, start, None, None, None, None, None)

  def apply(returns:Return, start:Start, matching:Match) = new Query(returns, start, Some(matching), None, None, None, None)
  def apply(returns:Return, start:Start, where:Clause) = new Query(returns, start, None, Some(where), None, None, None)
  def apply(returns:Return, start:Start, aggregation:Aggregation) = new Query(returns, start, None, None, Some(aggregation), None, None)
  def apply(returns:Return, start:Start, sort:Sort) = new Query(returns, start, None, None, None, Some(sort), None)

  def apply(returns:Return, start:Start, matching:Match, where:Clause) = new Query(returns, start, Some(matching), Some(where), None, None, None)
  def apply(returns:Return, start:Start, matching:Match, aggregation:Aggregation) = new Query(returns, start, Some(matching), None, Some(aggregation), None, None)
  def apply(returns:Return, start:Start, matching:Match, sort:Sort) = new Query(returns, start, Some(matching), None, None, Some(sort), None)

  def apply(returns:Return, start:Start, where:Clause, aggregation:Aggregation) = new Query(returns, start, None, Some(where), Some(aggregation), None, None)
  def apply(returns:Return, start:Start, where:Clause, sort:Sort) = new Query(returns, start, None, Some(where), None, Some(sort), None)

  def apply(returns:Return, start:Start, slice:Slice) = new Query(returns, start, None, None, None, None, Some(slice))

  def apply(returns:Return, start:Start, matching:Match, where:Clause, aggregation:Aggregation) = new Query(returns, start, Some(matching), Some(where), Some(aggregation), None, None)
}


case class Query(returns: Return, start: Start, matching:Option[Match], where: Option[Clause], aggregation: Option[Aggregation],
                 sort:Option[Sort], slice:Option[Slice])

case class Return(returnItems: ReturnItem*)

case class Start(startItems: StartItem*)

case class Match(patterns: Pattern*)

case class Aggregation(aggregationItems:AggregationItem*)

case class Sort(sortItems:SortItem*)

case class Slice(itemsToReturn:Int)