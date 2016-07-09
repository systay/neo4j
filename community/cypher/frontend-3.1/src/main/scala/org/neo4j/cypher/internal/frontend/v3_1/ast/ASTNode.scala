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

import org.neo4j.cypher.internal.frontend.v3_1.Rewritable._
import org.neo4j.cypher.internal.frontend.v3_1.symbols._
import org.neo4j.cypher.internal.frontend.v3_1._
import org.neo4j.cypher.internal.frontend.v3_1.ast.Atom.atom


trait ASTNode
  extends Product
  with Foldable
  with Rewritable {

  self =>

  /**
    * This is the root of the AST. When returning an AST, you must make sure to
    * mark it with `markThisAsRoot()`, so the correct root is set. Failure to do
    * so will result in exceptions
    */
  val root: Atom[ASTNode] = atom[ASTNode] { this }

  /**
    * Goes through the AST and makes sure all nodes know how to find the root.
    *
    * This is not the prettiest solution, and there probably exists an better, more functional approach.
    * For now, this will have to do.
    */
  def markThisAsRoot() = {
    this.findByAllClass[ASTNode].foreach {
      ast => ast.root.update(this)
    }
  }

  /**
    * The position in the input string of this object in the AST.
    * Any object in the tree missing position, e.g. because of rewriting,
    * will reach up in the tree and copy the position of the containing object
    */
  val position: Atom[InputPosition] = atom[InputPosition] {
    root().pushDownPositions()
  }

  protected def pushDownPositions(): Unit = {

    def doIt(astNode: ASTNode ) = {
      position.copyTo(astNode.position)
      astNode.pushDownPositions()
    }

    productIterator foreach {
      case astNode: ASTNode => doIt(astNode)
      case m: Map[_, ASTNode] => m.values.foreach(doIt)
      case iter: Iterable[ASTNode] => iter.foreach(doIt)
      case _ =>
    }
  }

  def dup(children: Seq[AnyRef]): this.type =
    if (children.iterator eqElements this.children)
      this
    else {
      val constructor = this.copyConstructor
      val params = constructor.getParameterTypes
      val args = children.toVector
      val hasExtraParam = params.length == args.length + 1
      val lastParamIsPos = params.last.isAssignableFrom(classOf[InputPosition])
      val ctorArgs = if (hasExtraParam && lastParamIsPos) args :+ this.position else args
      val duped = constructor.invoke(this, ctorArgs: _*)
      val result = duped.asInstanceOf[self.type]
      this.position.copyTo(result.position)
      result
    }
}

sealed trait ASTNodeType { self: ASTNode => }

trait ASTExpression extends ASTNodeType { self: ASTNode => }
trait ASTParticle extends ASTNodeType { self: ASTNode => }
trait ASTPhrase extends ASTNodeType { self: ASTNode => }

// Skip/Limit
trait ASTSlicingPhrase extends ASTPhrase with SemanticCheckable {
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
      case lit: Literal => SemanticError(s"Invalid input '${lit.asCanonicalStringVal}' is not a valid value, must be " +
        s"a positive integer", lit.position())
      case _ => SemanticCheckResult.success
    }
  }
}
