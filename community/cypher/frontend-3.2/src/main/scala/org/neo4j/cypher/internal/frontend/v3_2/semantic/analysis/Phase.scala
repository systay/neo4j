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
package org.neo4j.cypher.internal.frontend.v3_2.semantic.analysis

import org.neo4j.cypher.internal.frontend.v3_2.ast._

trait Phase[ENV] {
  def enrich(node: ASTNode): Unit = visit(node, initialValue)

  def visit(node: ASTNode, environment: ENV): ENV = {
    val beforeEnv = before(node, environment)
    val envAfterChildren = node.myChildren.foldLeft(beforeEnv) {
      case (env, child) => visit(child, env)
    }

    after(node, envAfterChildren)
  }

  def initialValue: ENV

  protected def before(node: ASTNode, environment: ENV): ENV

  protected def after(node: ASTNode, environment: ENV): ENV
}

object SemanticAnalysis {
  def visit(n: ASTNode): Unit = {
    Scoping.enrich(n)
    VariableBinding.enrich(n)
    Typing.enrich(n)
  }
}