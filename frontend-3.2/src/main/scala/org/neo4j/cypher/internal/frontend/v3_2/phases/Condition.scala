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
package org.neo4j.cypher.internal.frontend.v3_2.phases

import org.neo4j.cypher.internal.frontend.v3_2.SemanticState
import org.neo4j.cypher.internal.frontend.v3_2.ast.Statement

import scala.reflect.ClassTag

trait Condition {
  def check(state: AnyRef): Seq[String]
}

case class BaseContains[T: ClassTag](implicit manifest: Manifest[T]) extends Condition {
  private val acceptableTypes: Set[Class[_]] = Set(
    classOf[Statement],
    classOf[SemanticState]
  )

  assert(acceptableTypes.contains(manifest.runtimeClass))

  override def check(in: AnyRef): Seq[String] = in match {
    case state: BaseState =>
      manifest.runtimeClass match {
        case x if classOf[Statement] == x && state.maybeStatement.isEmpty => Seq("Statement missing")
        case x if classOf[SemanticState] == x && state.maybeSemantics.isEmpty => Seq("Semantic State missing")
        case _ => Seq.empty
      }
    case x => throw new IllegalArgumentException(s"Unknown state: $x")
  }
}
