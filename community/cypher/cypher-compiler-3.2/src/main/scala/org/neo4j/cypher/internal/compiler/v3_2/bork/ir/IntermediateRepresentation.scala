package org.neo4j.cypher.internal.compiler.v3_2.bork.ir

import org.neo4j.cypher.internal.compiler.v3_2.bork.Slot
import org.neo4j.cypher.internal.frontend.v3_2.ast

sealed trait IntermediateRepresentation

case class AllNodesScan(registerOffset: Int) extends IntermediateRepresentation
case class NodeByLabelScan(registerOffset: Int) extends IntermediateRepresentation
case class Filter(predicate: ast.Expression) extends IntermediateRepresentation
case class ProduceResults(columns: Map[String, Slot]) extends IntermediateRepresentation