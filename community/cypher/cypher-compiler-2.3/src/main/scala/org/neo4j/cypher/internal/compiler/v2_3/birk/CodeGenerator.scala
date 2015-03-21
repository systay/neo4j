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
package org.neo4j.cypher.internal.compiler.v2_3.birk

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.graphdb.Direction

import scala.collection.immutable.Stack
import scala.collection.mutable.ListBuffer


class CodeGenerator {
  def generate(plan: LogicalPlan): String = {
    val planStatements = createResultAst(plan)
    generateCodeFromAst(planStatements)
  }

  private def generateCodeFromAst(statements: Seq[ResultAst]) = {
    val imports = statements.flatMap(_.importedClasses()).distinct.sorted.mkString("import ", ";\nimport ", ";")
    val init = statements.map(_.generateInit()).reduce(_ + "\n" + _)
    val methodBody = statements.map(_.generateCode()).reduce(_ + "\n" + _)

    s"""$imports
       |
       |public class APA {
       |  public void execute(Statement statement, ResultVisitor visitor) {
       |    // INIT DATA STRUCTURES
       |    $init
       |
       |    // DO IT!
       |    $methodBody
       |  }
       |}""".stripMargin
  }

  private def createResultAst(plan: LogicalPlan):Seq[ResultAst] = {
    var varCounter = 0
    var variables: Map[String, String] = Map.empty
    var probeTables: Map[NodeHashJoin, String] = Map.empty
    val builder = new ListBuffer[ResultAst]

    def createVariableName() = {
      varCounter += 1
      s"v$varCounter"
    }

    def produce(plan: LogicalPlan, stack: Stack[LogicalPlan]) {
      plan match {
        case AllNodesScan(id, arguments) =>
          val variable = createVariableName()
          variables = variables + (id.name -> variable)
          val actions = consume(stack.top, plan, stack.pop)
          builder += WhileLoop(variable, ScanAllNodes(), actions)

        case _: ProduceResult | _: Expand =>
          produce(plan.lhs.get, stack.push(plan))

        case NodeHashJoin(_, lhs, rhs) =>
          produce(lhs, stack.push(plan))
          produce(rhs, stack.push(plan))
      }
    }

    def consume(plan: LogicalPlan, from: LogicalPlan, stack: Stack[LogicalPlan]): ResultAst = {
      plan match {
        case ProduceResult(columns, _) =>
          ProduceResults(columns.map(c => c -> variables(c)).toMap)

        case Expand(_, IdName(f), dir, _, IdName(to), IdName(rel), _) =>
          val nodeVar = createVariableName()
          val relVar = createVariableName()
          variables = variables + (rel -> relVar) + (to -> nodeVar)
          val action = consume(stack.top, plan, stack.pop)
          val fromVar = variables(f)
          WhileLoop(relVar, ExpandC(fromVar, relVar, nodeVar, dir), action)

        case join@NodeHashJoin(nodes, lhs, rhs) if from == lhs =>
          val nodeId = variables(nodes.head.name)
          val probeTableName = createVariableName()
          probeTables = probeTables. + (join -> probeTableName)
          BuildProbeTable(probeTableName, nodeId)

        case join@NodeHashJoin(_, lhs, rhs) if from == rhs =>
          val action = consume(stack.top, plan, stack.pop)
          val matches = createVariableName()
          val probeTableName = probeTables(join)
          WhileLoop(matches, GetMatchesFromProbeTable(probeTableName), action)
      }
    }

    produce(plan, Stack.empty)

    builder.toSeq
  }
}

trait ResultAst {
  // Actual code produced by element
  def generateCode(): String

  // Initialises necessary data-structures. Is inserted at the top of the generated method
  def generateInit(): String

  // Generates import list for class
  def importedClasses(): Vector[String]
}

// Generates the code that moves data into local variables from the iterator being consumed
trait LoopDataGenerator extends ResultAst {
  def generateVariablesAndAssignment(): String
}

case class WhileLoop(id: String, producer: LoopDataGenerator, action: ResultAst) extends ResultAst {
  def generateCode(): String = {
    val iterator = s"${id}Iter"

    s"""  PrimitiveLongIterator $iterator = ${producer.generateCode()};
       |  while ( $iterator.hasNext() ) {
       |    long $id = $iterator.next();
       |    ${producer.generateVariablesAndAssignment()}
       |    ${action.generateCode()}
       |  }
       |""".stripMargin
  }

  def generateInit() = producer.generateInit() + action.generateInit()

  def importedClasses() =
    producer.importedClasses() ++
    action.importedClasses() :+
      "org.neo4j.collection.primitive.PrimitiveLongIterator"
}

case class ScanAllNodes() extends ResultAst with LoopDataGenerator {
  def generateCode() = "ro.nodesGetAll()"

  def generateVariablesAndAssignment() = ""

  def generateInit() = ""

  def importedClasses() = Vector.empty
}

case class ProduceResults(columns: Map[String, String]) extends ResultAst {
  def generateCode() = columns.toSeq.map {
    case (k, v) => s"""    row.setNodeId("$k", $v);"""
  }.mkString("\n") + "\n    visitor.accept(row);\n"

  def generateInit() = ""

  def importedClasses() = Vector.empty
}

case class ExpandC(from: String, relVar: String, nodeVar: String, dir: Direction) extends LoopDataGenerator {
  def generateCode(): String = s"ro.nodeGetRelationships($from, Direction.$dir)"

  def generateVariablesAndAssignment(): String = s"long $nodeVar = 666; // Should get the other end of the relationship"

  def generateInit() = ""

  // Generates import list for class
  def importedClasses() = Vector.empty
}

case class BuildProbeTable(name: String, node: String) extends ResultAst {
  def generateInit() = s"PrimitiveLongIntMap $name = Primitive.longIntMap();"

  def generateCode() =
    s"""    int count = $name.get( $node );
       |    if ( count == LongKeyIntValueTable.NULL ) {
       |        $name.put( $node, 1 );
       |    } else {
       |        $name.put( $node, count + 1 );
       |    }""".stripMargin

  // Generates import list for class
  def importedClasses() = Vector("org.neo4j.collection.primitive.PrimitiveLongIntMap", "org.neo4j.collection.primitive.hopscotch.LongKeyIntValueTable")
}

case class GetMatchesFromProbeTable(probeTable: String) extends LoopDataGenerator {
  // Initialises necessary data-structures. Is inserted at the top of the generated method
  def generateInit() = ""

  // Generates import list for class
  def importedClasses() = Vector.empty

  def generateCode() = "GET MATCHES FROM PROBE TABLE"

  def generateVariablesAndAssignment() = "APA!"
}
