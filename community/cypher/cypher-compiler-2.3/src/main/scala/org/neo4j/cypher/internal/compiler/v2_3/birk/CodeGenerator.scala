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

import java.util.concurrent.atomic.AtomicInteger

import org.neo4j.cypher.internal.compiler.v2_3.birk.il._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._

import scala.collection.immutable.Stack

object CodeGenerator {
  private val nameCounter = new AtomicInteger(0)

  def nextClassName(): String = {
    val x = nameCounter.getAndIncrement
    s"GeneratedExecutionPlan$x"
  }

  def indentNicely(in: String): String = {

    var indent = 0

    in.split(n).flatMap {
      line =>
        val l = line.stripPrefix(" ")
        if (l == "")
          None
        else {
          if (l == "}")
            indent = indent - 1

          val result = "  " * indent + l

          if (l == "{")
            indent = indent + 1

          Some(result)
        }
    }.mkString(n)
  }

  def n = System.lineSeparator()
}

class CodeGenerator {

  import CodeGenerator.n

  def generate(plan: LogicalPlan): String = {
    val planStatements: Seq[Instruction] = createResultAst(plan)
    val source = generateCodeFromAst(CodeGenerator.nextClassName(), planStatements)
    CodeGenerator.indentNicely(source)
  }

  private def generateCodeFromAst(className: String, statements: Seq[Instruction]) = {
    val importLines = statements.flatMap(_.importedClasses())
    val imports = if(importLines.nonEmpty)
      importLines.distinct.sorted.mkString("import ", s";${n}import ", ";")
    else
      ""
    val init = statements.map(_.generateInit()).reduce(_ + n + _)
    val methodBody = statements.map(_.generateCode()).reduce(_ + n + _)
    val privateMethods = statements.flatMap(_.methods).distinct.sortBy(_.name)
    val privateMethodText = privateMethods.map(_.generateCode).reduceOption(_ + n + _).getOrElse("")

    s"""package org.neo4j.cypher.internal.compiler.v2_3.birk.generated;
       |
       |import org.neo4j.helpers.collection.Visitor;
       |import org.neo4j.kernel.api.Statement;
       |import org.neo4j.kernel.api.exceptions.KernelException;
       |import org.neo4j.cypher.internal.compiler.v2_3.birk.ExecutablePlan
       |import org.neo4j.kernel.api.ReadOperations;
       |$imports
       |
       |public class $className implements ExecutablePlan
       |{
       |@Override
       |public void accept(Visitor visitor, Statement statement) throws KernelException
       |{
       |ReadOperations ro = statement.readOperations();
       |$init
       |$methodBody
       |}
       |$privateMethodText
       |}""".stripMargin
  }


  class Namer(prefix: String) {
    var varCounter = 0

    def next() = {
      varCounter += 1
      prefix + varCounter
    }
  }

  case class Symbol(id: String, javaType: String)

  private def createResultAst(plan: LogicalPlan): Seq[Instruction] = {
    var variables: Map[String, String] = Map.empty
    var probeTables: Map[NodeHashJoin, String] = Map.empty
    val variableName = new Namer("v")
    val methodName = new Namer("m")

    def produce(plan: LogicalPlan, stack: Stack[LogicalPlan]): (Option[Symbol], Seq[Instruction]) = {
      plan match {
        case AllNodesScan(id, arguments) =>
          val variable = variableName.next()
          variables = variables + (id.name -> variable)
          val (apa, actions) = consume(stack.top, plan, stack.pop)
          (apa, Seq(WhileLoop(variable, ScanAllNodes(), actions)))

        case _: ProduceResult | _: Expand =>
          produce(plan.lhs.get, stack.push(plan))

        case NodeHashJoin(_, lhs, rhs) =>
          val (Some(symbol), lAst) = produce(lhs, stack.push(plan))
          val lhsMethod = MethodInvocation(symbol.id, symbol.javaType, methodName.next(), lAst)
          val (x, r) = produce(rhs, stack.push(plan))
          (x, lhsMethod +: r)
      }
    }

    def consume(plan: LogicalPlan, from: LogicalPlan, stack: Stack[LogicalPlan]): (Option[Symbol], Instruction) = {
      plan match {
        case ProduceResult(columns, _) =>
          (None, ProduceResults(columns.map(c => c -> variables(c)).toMap))

        case Expand(_, IdName(f), dir, _, IdName(to), IdName(rel), _) =>
          val nodeVar = variableName.next()
          val relVar = variableName.next()
          variables = variables + (rel -> relVar) + (to -> nodeVar)
          val (variable,action) = consume(stack.top, plan, stack.pop)
          val fromVar = variables(f)
          (variable, WhileLoop(relVar, ExpandC(fromVar, relVar, nodeVar, dir), action))

        case join@NodeHashJoin(nodes, lhs, rhs) if from == lhs =>
          val nodeId = variables(nodes.head.name)
          val probeTableName = variableName.next()
          probeTables = probeTables. + (join -> probeTableName)
          (Some(Symbol(probeTableName, BuildProbeTable.producedType)), BuildProbeTable(probeTableName, nodeId))

        case join@NodeHashJoin(_, lhs, rhs) if from == rhs =>
          val (x, action) = consume(stack.top, plan, stack.pop)
          val matches = variableName.next()
          val probeTableName = probeTables(join)
          (x, WhileLoop(matches, GetMatchesFromProbeTable(probeTableName), action))
      }
    }

    val (_, result) = produce(plan, Stack.empty)

    result
  }
}
