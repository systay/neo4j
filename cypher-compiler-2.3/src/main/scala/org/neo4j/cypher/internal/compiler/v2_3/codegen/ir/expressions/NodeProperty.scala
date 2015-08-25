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
package org.neo4j.cypher.internal.compiler.v2_3.codegen.ir.expressions

import org.neo4j.cypher.internal.compiler.v2_3.codegen.{CodeGenContext, MethodStructure, Variable}
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

abstract class ElementProperty(token: Option[Int], propName: String, elementIdVar: String, propKeyVar: String)
  extends CodeGenExpression {
  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    if (token.isEmpty) generator.lookupPropertyKey(propName, propKeyVar)

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext): E = {
    val localName = context.namer.newVarName()
    structure.declareProperty(localName)
    if (token.isEmpty)
      propertyByName(structure, localName)
    else
      propertyById(structure, localName)
    structure.incrementDbHits()
    structure.load(localName)
  }

  def propertyByName[E](body: MethodStructure[E], localName: String): Unit

  def propertyById[E](body: MethodStructure[E], localName: String): Unit

  override def nullable(implicit context: CodeGenContext) = true
}

case class NodeProperty(token: Option[Int], propName: String, nodeIdVar: Variable, propKeyVar: String)
  extends ElementProperty(token, propName, nodeIdVar.name, propKeyVar) {

  override def propertyByName[E](body: MethodStructure[E], localName: String) =
    if (nodeIdVar.nullable)
      body.ifStatement(body.notNull(nodeIdVar.name, nodeIdVar.cypherType)) {ifBody =>
        ifBody.nodeGetPropertyForVar(nodeIdVar.name, propKeyVar, localName)
      }
    else
      body.nodeGetPropertyForVar(nodeIdVar.name, propKeyVar, localName)

  override def propertyById[E](body: MethodStructure[E], localName: String) =
    if (nodeIdVar.nullable)
      body.ifStatement(body.notNull(nodeIdVar.name, nodeIdVar.cypherType)) {ifBody =>
        ifBody.nodeGetPropertyById(nodeIdVar.name, token.get, localName)
      }
    else
      body.nodeGetPropertyById(nodeIdVar.name, token.get, localName)

  override def cypherType(implicit context: CodeGenContext) = CTAny
}

case class RelProperty(token: Option[Int], propName: String, relIdVar: Variable, propKeyVar: String)
  extends ElementProperty(token, propName, relIdVar.name, propKeyVar) {

  override def propertyByName[E](body: MethodStructure[E], localName: String) =
    if (relIdVar.nullable)
      body.ifStatement(body.notNull(relIdVar.name, relIdVar.cypherType)) { ifBody =>
        ifBody.relationshipGetPropertyForVar(relIdVar.name, propKeyVar, localName)
      }
    else
      body.relationshipGetPropertyForVar(relIdVar.name, propKeyVar, localName)

  override def propertyById[E](body: MethodStructure[E], localName: String) =
  if (relIdVar.nullable)
    body.ifStatement(body.notNull(relIdVar.name, relIdVar.cypherType)) { ifBody =>
      ifBody.relationshipGetPropertyById(relIdVar.name, token.get, localName)
    }
    else
      body.relationshipGetPropertyById(relIdVar.name, token.get, localName)

  override def cypherType(implicit context: CodeGenContext) = CTAny
}
