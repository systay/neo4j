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
package org.neo4j.cypher.internal.frontend.v2_3.parser

import org.neo4j.cypher.internal.frontend.v2_3.ast
import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.TestName
import org.parboiled.scala.Rule1

trait ParserAstTest[AST] extends ParserTest[AST, AST] with TestName {
  final override def convert(ast: AST): AST = ast

  final def yields(expr: AST)(implicit parser: Rule1[AST]) = parsing(testName) shouldGive expr

  final def id(id: String) = ast.Identifier(id)

  final def lt(lhs: Expression, rhs: Expression): Expression = ast.LessThan(lhs, rhs)

  final def lte(lhs: Expression, rhs: Expression): Expression = ast.LessThanOrEqual(lhs, rhs)

  final def gt(lhs: Expression, rhs: Expression): Expression = ast.GreaterThan(lhs, rhs)

  final def gte(lhs: Expression, rhs: Expression): Expression = ast.GreaterThanOrEqual(lhs, rhs)

  final def eq(lhs: Expression, rhs: Expression): Expression = ast.Equals(lhs, rhs)

  final def ne(lhs: Expression, rhs: Expression): Expression = ast.NotEquals(lhs, rhs)

  final def and(lhs: Expression, rhs: Expression): Expression = ast.And(lhs, rhs)

  final def ands(parts: Expression*): Expression = ast.Ands(parts.toSet)
}
