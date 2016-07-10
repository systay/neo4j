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
package org.neo4j.cypher.internal.frontend.v3_1.ast

import org.neo4j.cypher.internal.frontend.v3_1.{SemanticCheckResult, SemanticError, _}
import org.neo4j.cypher.internal.frontend.v3_1.symbols._

// Skip/Limit
sealed trait ASTSlicingPhrase extends SemanticCheckable {
  self: ASTNode =>
  def name: String
  def dependencies = expression.dependencies
  def expression: Expression

  def semanticCheck =
    containsNoVariables chain
      literalShouldBeUnsignedInteger chain
      expression.semanticCheck(Expression.SemanticContext.Simple) chain
      expression.expectType(CTInteger.covariant)

  private def containsNoVariables: SemanticCheck = {
    val deps = dependencies
    if (deps.nonEmpty) {
      val id = deps.toSeq.sortBy(_.position()).head
      SemanticError(s"It is not allowed to refer to variables in $name", id.position)
    }
    else SemanticCheckResult.success
  }

  private def literalShouldBeUnsignedInteger: SemanticCheck = {
    expression match {
      case _: UnsignedDecimalIntegerLiteral => SemanticCheckResult.success
      case i: SignedDecimalIntegerLiteral if i.value >= 0 => SemanticCheckResult.success
      case lit: Literal =>
        val message = s"Invalid input '${lit.asCanonicalStringVal}' is not a valid value, must be a positive integer"
        SemanticError(message, lit.position())
      case _ => SemanticCheckResult.success
    }
  }
}

case class Limit(expression: Expression) extends ASTNode with ASTSlicingPhrase {
  override def name = "LIMIT" // ASTSlicingPhrase name
}

case class Skip(expression: Expression) extends ASTNode with ASTSlicingPhrase {
  override def name = "SKIP" // ASTSlicingPhrase name
}