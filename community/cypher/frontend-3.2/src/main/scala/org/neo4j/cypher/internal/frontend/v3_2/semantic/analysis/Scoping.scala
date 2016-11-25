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

import org.neo4j.cypher.internal.frontend.v3_2.InvalidSemanticsException
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.semantic.analysis.Scope.ScopingContext
import org.neo4j.cypher.internal.frontend.v3_2.symbols.TypeSpec

import scala.collection.mutable.ListBuffer

object Scoping extends Phase[Scope] {

  override def initialValue: Scope = Scope.empty

  override protected def before(node: ASTNode, environment: Scope): Scope = {
    node.myScope.value = (environment, environment.context)

    node match {
      case _: SingleQuery =>
        environment.enterScope(ScopingContext.Default)

      case foreach: Foreach =>
        visit(foreach.expression, environment.changeContext(ScopingContext.Expression))
        environment.enterScope(ScopingContext.Default).add(foreach.variable)

      case _: Match =>
        environment.changeContext(ScopingContext.Match)

      case _: Return =>
        environment.changeContext(ScopingContext.Expression)

      case v: Variable if environment.context == ScopingContext.Match =>
        environment.add(v)

      case v: Variable if environment.context == ScopingContext.Expression && !environment.variableDefined(v) =>
        throw new InvalidSemanticsException(s"Variable `${v.name}` not declared, at ${v.position}")

      case _ =>
        environment
    }
  }

  override protected def after(node: ASTNode, environment: Scope): Scope = node match {
    case _: SingleQuery | _: Foreach =>
      environment.exitScope()

    case _ =>
      environment
  }
}

class Scope(val outer: Scope, var context: ScopingContext) {
  private val locals = new ListBuffer[Variable]
  private val types = scala.collection.mutable.HashMap[Variable, TypeSpec]()

  def add(l: Variable): Scope = {
    locals += l
    this
  }

  def setTypeTo(v: Variable, t: TypeSpec): Unit = types.put(v, t)

  def getTypeOf(v: Variable): TypeSpec = types(v)

  def enterScope(context: ScopingContext) = new Scope(this, context)

  def exitScope(): Scope = {
    assert(outer != null, "Tried to exit the outermost scope")
    outer
  }

  def changeContext(context: ScopingContext): Scope = {
    this.context = context
    this
  }

  def variableDefined(v: Variable): Boolean = {
    if (locals.contains(v)) return true
    if (outer == null) return false
    outer.variableDefined(v)
  }

  // Equality and toString
  override def toString: String = locals.map(_.name).mkString(outer.toString + "[", ", ", "]")

  def canEqual(other: Any): Boolean = other.isInstanceOf[Scope]

  override def equals(other: Any): Boolean = other match {
    case that: Scope =>
      (that canEqual this) &&
        locals == that.locals &&
        outer == that.outer
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(locals, outer)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object Scope {
  def empty = new Scope(null, ScopingContext.Default) {
    override def toString = "[]"
  }

  sealed trait ScopingContext

  object ScopingContext {

    case object Default extends ScopingContext

    case object Match extends ScopingContext

    case object Merge extends ScopingContext

    case object Create extends ScopingContext

    case object CreateUnique extends ScopingContext

    case object Expression extends ScopingContext

  }

}
