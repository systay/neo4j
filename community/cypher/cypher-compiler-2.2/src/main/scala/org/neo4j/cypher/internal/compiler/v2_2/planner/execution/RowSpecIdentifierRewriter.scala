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
package org.neo4j.cypher.internal.compiler.v2_2.planner.execution

import org.neo4j.cypher.internal.compiler.v2_2.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.cypher.internal.compiler.v2_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.{Identifier, Expression}

case class RowSpecIdentifierRewriter(spec: RowSpec) extends (Expression => Expression) {
  def apply(e: Expression): Expression = e match {
    case Identifier(id) => spec.indexTo(id) match {
      case Some(NodeIndex(idx))  => NodeIdentifier(idx)
      case Some(RelIndex(idx))   => RelationshipIdentifier(idx)
      case Some(OtherIndex(idx)) => AnyRefIdentifier(idx)
    }
    case _ => e
  }
}

case class NodeIdentifier(idx: Int) extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = ctx.getNode(idx)
  def rewrite(f: (Expression) => Expression): Expression = f(this)
  def arguments: Seq[Expression] = Seq.empty
  protected def calculateType(symbols: SymbolTable): CypherType = CTNode
  def symbolTableDependencies: Set[String] = Set.empty
}

case class RelationshipIdentifier(idx: Int) extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = ctx.getRelationship(idx)
  def rewrite(f: (Expression) => Expression): Expression = f(this)
  def arguments: Seq[Expression] = Seq.empty
  protected def calculateType(symbols: SymbolTable): CypherType = CTNode
  def symbolTableDependencies: Set[String] = Set.empty
}

case class AnyRefIdentifier(idx: Int) extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = ctx.getAnyRef(idx)
  def rewrite(f: (Expression) => Expression): Expression = f(this)
  def arguments: Seq[Expression] = Seq.empty
  protected def calculateType(symbols: SymbolTable): CypherType = CTNode
  def symbolTableDependencies: Set[String] = Set.empty
}
