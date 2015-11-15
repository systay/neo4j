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
package org.neo4j.cypher.internal.compiler.v3_0.pipes

import org.neo4j.cypher.internal.compiler.v3_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_0.helpers.CollectionSupport
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.{InternalPlanDescription, PlanDescriptionImpl, SingleChild}

case class UnwindPipe(source: Pipe, collection: Expression, variable: String)
                     (val estimatedCardinality: Option[Double] = None)(implicit monitor: PipeMonitor)
  extends PipeWithSource(source, monitor) with CollectionSupport with RonjaPipe {
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    //register as parent so that stats are associated with this pipe
    state.decorator.registerParentPipe(this)

    input.flatMap {
      context =>
        val seq = makeTraversable(collection(context)(state))
        seq.map(x => context.newWith1(variable, x))
    }
  }

  def planDescriptionWithoutCardinality: InternalPlanDescription =
    PlanDescriptionImpl(this.id, "UNWIND", SingleChild(source.planDescription), Seq(), variables)

  def symbols = source.symbols.add(variable, collection.getType(source.symbols).legacyIteratedType)

  override def localEffects = collection.effects(symbols)

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)(estimatedCardinality)
  }

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}
