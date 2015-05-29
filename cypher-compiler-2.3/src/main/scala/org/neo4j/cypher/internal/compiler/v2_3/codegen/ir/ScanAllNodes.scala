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
package org.neo4j.cypher.internal.compiler.v2_3.codegen.ir

import org.neo4j.cypher.internal.compiler.v2_3.codegen.MethodStructure

case class ScanAllNodes(id: String) extends Instruction with LoopDataGenerator {

  override def produceIterator[E](iterVar: String, generator: MethodStructure[E]) = {
    generator.allNodesScan(iterVar)
    generator.incrementDbHits()
  }

  override def produceNext[E](nextVar: String, iterVar: String, generator: MethodStructure[E]) =
    generator.nextNode(nextVar, iterVar)

  def generateCode() = "ro.nodesGetAll()"

  def generateVariablesAndAssignment() = ""

  def generateInit() = ""

  override protected def importedClasses = Set("org.neo4j.collection.primitive.PrimitiveLongIterator")

  def javaType = "PrimitiveLongIterator"

  override protected def children = Seq.empty
}
