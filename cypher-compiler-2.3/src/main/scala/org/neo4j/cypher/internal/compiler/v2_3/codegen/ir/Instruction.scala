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

import org.neo4j.cypher.internal.compiler.v2_3.codegen.{ExceptionCodeGen, MethodStructure}

trait Instruction {
  def init[E](generator: MethodStructure[E]): Unit = children.foreach(_.init(generator))
  def body[E](generator: MethodStructure[E]): Unit = ???

  protected def children: Seq[Instruction]

  private def treeView: Seq[Instruction] = {
    children.foldLeft(Seq(this)) { (acc, child) => acc ++ child.treeView }
  }

  // Aggregating methods -- final to prevent overriding
  final def allOperatorIds: Set[String] = treeView.flatMap(_.operatorId).toSet

  final def allColumns: Set[String] = treeView.flatMap(_.columnNames).toSet

  protected def operatorId: Option[String] = None

  protected def columnNames: Iterable[String] = None
}

object Instruction {

  val empty = new Instruction {
    override def body[E](generator: MethodStructure[E]) = {}

    override protected def children = Seq.empty
  }
}
