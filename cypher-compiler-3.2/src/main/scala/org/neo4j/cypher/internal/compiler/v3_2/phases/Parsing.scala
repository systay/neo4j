/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.CompilationPhase.PARSING
import org.neo4j.cypher.internal.frontend.v3_2.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_2.parser.CypherParser
import org.neo4j.cypher.internal.frontend.v3_2.phases.{BaseContains, BaseContext, BaseState}

case object Parsing extends Phase[BaseContext, BaseState, BaseState] {
  private val parser = new CypherParser

  override def process(in: BaseState, ignored: BaseContext): BaseState =
    CompilationState(in).copy(maybeStatement =
      Some(parser.parse(in.queryText, in.startPosition)))

  override val phase = PARSING

  override val description = "parse text into an AST object"

  override def postConditions = Set(BaseContains[Statement])
}
