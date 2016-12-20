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
package org.neo4j.cypher.internal.compiler.v3_2.phases

import org.neo4j.cypher.internal.compiler.v3_2.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.compiler.v3_2.helpers.closing

trait Phase extends Transformer {
  self =>

  def phase: CompilationPhase

  def description: String

  def transformReporting(from: CompilationState, context: Context): CompilationState =
    closing(context.tracer.beginPhase(phase)) {
      transform(from, context)
    }

  def postConditions: Set[Condition]
}

trait VisitorPhase extends Phase {
  override def transform(from: CompilationState, context: Context): CompilationState = {
    visit(from, context)
    from
  }

  def visit(value: CompilationState, context: Context): Unit

  override def postConditions: Set[Condition] = Set.empty
}

trait Transformer {
  def transform(from: CompilationState, context: Context): CompilationState

  def andThen(other: Transformer) =
    new PipeLine(this, other)
}

object Transformer {
  val identity = new Transformer {
    override def transform(from: CompilationState, context: Context) = from
  }
}

class PipeLine(first: Transformer, after: Transformer) extends Transformer {

  override def transform(from: CompilationState, context: Context): CompilationState = {
    var step = first.transform(from, context)
    var messages: Set[String] = Set.empty
    assert({
      // Checking conditions inside assert so they are not run in production
      // TODO: Make sure this is really only run if assertions are enabled
      step = addConditions(step, first)
      messages = step.accumulatedConditions.flatMap(condition => condition.check(step))
      messages.isEmpty // If this is not true, we will fail if run with -ea
    }, messages.mkString(", "))
    step = after.transform(step, context)
    assert({
      step = addConditions(step, after)
      messages = step.accumulatedConditions.flatMap(condition => condition.check(step))
      messages.isEmpty
    }, messages.mkString(", "))
    step
  }

  private def addConditions(state: CompilationState, transformer: Transformer): CompilationState = {
    transformer match {
      case phase: Phase => state.copy(accumulatedConditions = state.accumulatedConditions ++ phase.postConditions)
      case _ => state
    }
  }
}

case class If(f: CompilationState => Boolean)(thenT: Transformer) {
  def orElse(elseT: Transformer) = new Transformer {
    override def transform(from: CompilationState, context: Context): CompilationState =
      if (f(from))
        thenT.transform(from, context)
      else
        elseT.transform(from, context)
  }
}