/**
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
package org.neo4j.cypher.internal.compiler.v2_3.birk.il

import org.neo4j.graphdb.Direction

case class ExpandC(from: String, relVar: String, nodeVar: String, dir: Direction, inner: Instruction) extends LoopDataGenerator {
  def generateCode() = s"ro.nodeGetRelationships($from, Direction.$dir)"

  def generateVariablesAndAssignment() =
    s"""ro.relationshipVisit( $relVar, new RelationshipVisitor<KernelException>()
       |{
       |@Override
       |public void visit( long relId, int type, long startNode, long $nodeVar ) throws KernelException
       |{
       |${inner.generateCode()}
       |}
       |});""".stripMargin

  override def _importedClasses() =  Set(
    "org.neo4j.graphdb.Direction",
    "org.neo4j.collection.primitive.PrimitiveLongIterator",
    "org.neo4j.kernel.api.exceptions.KernelException",
    "org.neo4j.kernel.impl.api.RelationshipVisitor")

  def javaType = "PrimitiveLongIterator"

  def generateInit() = inner.generateInit()

  override def fields() = inner.fields()

  override def children = Seq(inner)

}
