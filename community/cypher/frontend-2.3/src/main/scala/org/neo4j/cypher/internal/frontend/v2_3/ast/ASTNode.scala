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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.Rewritable._
import org.neo4j.cypher.internal.frontend.v2_3.perty.PageDocFormatting
import org.neo4j.cypher.internal.frontend.v2_3.{InternalException, Foldable, InputPosition, Rewritable}

trait Positionable {
  // Points to where in the string this AST object comes from. Used for error messages
  protected var _position: InputPosition = null

  // Copies position to other AST node if set
  def copyPosTo[T <: Positionable](other: T): T = {
    if (_position != null)
      other.setPos(_position)
    other
  }

  def setPos(position: InputPosition): this.type = {
    if (_position != null) throw new InternalException("Position cannot be changed")
    _position = position
    this
  }

  def position: InputPosition = {
    if (_position != null) throw new InternalException("Position has not been set for this AST node")
    _position
  }
}

trait ASTNode
  extends Product
  with Foldable
  with Rewritable
  with Positionable
  with PageDocFormatting /* multi line */
  // with LineDocFormatting  /* single line */
//  with ToPrettyString[ASTNode]
{

  self =>

//  def toDefaultPrettyString(formatter: DocFormatter): String =
////    toPrettyString(formatter)(DefaultDocHandler.docGen) /* scala like */
//    toPrettyString(formatter)(InternalDocHandler.docGen) /* see there for more choices */

  /**
   * Handles deep copying of AST and still remembers the original InputPosition
   * @param children Arguments to the constructor
   * @return An AST node of same type, but with the new children given in arguments
   */
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
      duped.asInstanceOf[ASTNode].setPos(_position)
      duped.asInstanceOf[self.type]
    }
}

// This is used by pretty printing to distinguish between
//
// - expressions
// - particles (non-expression ast nodes contained in expressions)
// - terms (neither expressions nor particles, like Clause)
//
sealed trait ASTNodeType { self: ASTNode => }

trait ASTExpression extends ASTNodeType { self: ASTNode => }
trait ASTParticle extends ASTNodeType { self: ASTNode => }
trait ASTPhrase extends ASTNodeType { self: ASTNode => }

// Skip/Limit
trait ASTSlicingPhrase extends ASTPhrase { self: ASTNode => }
