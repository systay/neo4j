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

import org.neo4j.cypher.internal.compiler.v2_3.birk.CodeGenerator.JavaTypes.{LONG, INT}
import org.neo4j.cypher.internal.compiler.v2_3.birk.il._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._

import scala.collection.immutable.Stack

object CodeGenerator {
  private val nameCounter = new AtomicInteger(0)

  val classNamer = new Namer("GeneratedExecutionPlan")

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

  object JavaTypes {
    def LONG = "long"
    def INT = "int"
  }
}

class Namer(prefix: String) {
  var varCounter = 0

  def next(): String = {
    varCounter += 1
    prefix + varCounter
  }

  def nextWithType(typ: String): JavaSymbol = JavaSymbol(next(), typ)
}

case class JavaSymbol(name: String, javaType: String)

class CodeGenerator {

  import CodeGenerator.n

  def generate(plan: LogicalPlan, className: String = CodeGenerator.classNamer.next()): String = {
    val planStatements: Seq[Instruction] = createResultAst(plan)
    val source = generateCodeFromAst(className, planStatements)
    CodeGenerator.indentNicely(source)
  }

  private def generateCodeFromAst(className: String, statements: Seq[Instruction]) = {
    val importLines: Set[String] =
      statements.
        map(_.importedClasses()).
        reduceOption(_ ++ _).
        getOrElse(Set.empty)

    val imports = if(importLines.nonEmpty)
      importLines.toSeq.sorted.mkString("import ", s";${n}import ", ";")
    else
      ""
    val fields = statements.map(_.fields().trim).reduce(_ + n + _)
    val init = statements.map(_.generateInit().trim).reduce(_ + n + _)
    val methodBody = statements.map(_.generateCode().trim).reduce(_ + n + _)
    val privateMethods = statements.flatMap(_.methods).distinct.sortBy(_.name)
    val privateMethodText = privateMethods.map(_.generateCode.trim).reduceOption(_ + n + _).getOrElse("")

    s"""package org.neo4j.cypher.internal.compiler.v2_3.birk.generated;
       |
       |import org.neo4j.helpers.collection.Visitor;
       |import org.neo4j.graphdb.GraphDatabaseService;
       |import org.neo4j.kernel.api.Statement;
       |import org.neo4j.kernel.api.exceptions.KernelException;
       |import org.neo4j.kernel.api.ReadOperations;
       |import org.neo4j.cypher.internal.compiler.v2_3.birk.ExecutablePlan;
       |import org.neo4j.cypher.internal.compiler.v2_3.birk.ResultRow;
       |import org.neo4j.cypher.internal.compiler.v2_3.birk.ResultRowImpl;
       |$imports
       |
       |public class $className implements ExecutablePlan
       |{
       |
       |$fields
       |
       |@Override
       |public void accept(final Visitor<ResultRow, KernelException> visitor, final Statement statement, GraphDatabaseService db) throws KernelException
       |{
       |final ReadOperations ro = statement.readOperations();
       |final ResultRowImpl row = new ResultRowImpl(db);
       |$init
       |$methodBody
       |}
       |$privateMethodText
       |}""".stripMargin
  }

  private def createResultAst(plan: LogicalPlan): Seq[Instruction] = {
    var variables: Map[String, JavaSymbol] = Map.empty
    var probeTables: Map[NodeHashJoin, CodeThunk] = Map.empty
    val variableName = new Namer("v")
    val methodName = new Namer("m")

    def produce(plan: LogicalPlan, stack: Stack[LogicalPlan]): (Option[JavaSymbol], Seq[Instruction]) = {
      plan match {
        case AllNodesScan(IdName(name), arguments) =>
          val variable = variableName.nextWithType(LONG)
          variables += (name -> variable)
          val (methodHandle, actions) = consume(stack.top, plan, stack.pop)
          (methodHandle, Seq(WhileLoop(variable, ScanAllNodes(), actions)))

        case NodeByLabelScan(IdName(name), label, _) =>
          val nodeVariable = variableName.nextWithType(LONG)
          val labelToken = variableName.nextWithType(INT)
          variables += (name -> nodeVariable)
          val (methodHandle, actions) = consume(stack.top, plan, stack.pop)
          (methodHandle, Seq(WhileLoop(nodeVariable, ScanForLabel(label.name, labelToken), actions)))

        case _: ProduceResult | _: Expand =>
          produce(plan.lhs.get, stack.push(plan))

        case NodeHashJoin(_, lhs, rhs) =>
          val (Some(symbol), lAst) = produce(lhs, stack.push(plan))
          val lhsMethod = MethodInvocation(symbol.name, symbol.javaType, methodName.next(), lAst)
          val (x, r) = produce(rhs, stack.push(plan))
          (x, lhsMethod +: r)
      }
    }

    def consume(plan: LogicalPlan, from: LogicalPlan, stack: Stack[LogicalPlan]): (Option[JavaSymbol], Instruction) = {
      plan match {
        case ProduceResult(columns, _) =>
          (None, ProduceResults(columns.map(c => c -> variables(c).name).toMap))

        case join@NodeHashJoin(nodes, lhs, rhs) if from eq lhs =>
          val nodeId = variables(nodes.head.name)
          val probeTableName = variableName.next()
          val symbols = (lhs.availableSymbols.map(_.name) intersect variables.keySet diff nodes.map(_.name)).map(s => s -> variables(s)).toMap

          val probeTable = BuildProbeTable(probeTableName, nodeId.name, symbols, variableName)
          val probeTableSymbol = JavaSymbol(probeTableName, probeTable.producedType)

          probeTables += (join -> probeTable.generateFetchCode)

          (Some(probeTableSymbol), probeTable)

        case join@NodeHashJoin(nodes, lhs, rhs) if from eq rhs =>
          val nodeId = variables(nodes.head.name)
          val thunk = probeTables(join)
          thunk.vars foreach(variables += _)
          val (x, action) = consume(stack.top, plan, stack.pop)

          (x, GetMatchesFromProbeTable(nodeId.name, thunk, action))

        case Expand(_, IdName(fromNode), dir, _, IdName(to), IdName(rel), _) =>
          val relVar = variableName.nextWithType(LONG)
          val nodeVar = variableName.nextWithType(LONG)
          variables  += rel -> relVar
          variables  += to -> nodeVar

          val (x, action) = consume(stack.top, plan, stack.pop)
          (x, WhileLoop(relVar, ExpandC(variables(fromNode).name, relVar.name, nodeVar.name, dir, action), Instruction.empty))
      }
    }

    val (_, result) = produce(plan, Stack.empty)

    result
  }
}
