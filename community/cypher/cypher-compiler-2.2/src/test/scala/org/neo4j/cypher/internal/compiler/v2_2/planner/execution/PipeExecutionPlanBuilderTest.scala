/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.execution

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.LabelId
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.{Collection, RelTypeName, SignedDecimalIntegerLiteral, SignedIntegerLiteral}
import org.neo4j.cypher.internal.compiler.v2_2.commands.{expressions => legacy}
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.PipeInfo
import org.neo4j.cypher.internal.compiler.v2_2.pipes.{EntityByIdExprs => PipeEntityByIdExprs, _}
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{EntityByIdExprs => PlanEntityByIdExprs, _}
import org.neo4j.graphdb.Direction

class PipeExecutionPlanBuilderTest extends CypherFunSuite with LogicalPlanningTestSupport {

  implicit val planContext = newMockedPlanContext
  implicit val pipeMonitor = monitors.newMonitor[PipeMonitor]()
  implicit val LogicalPlanningContext = newMockedLogicalPlanningContext(planContext)
  implicit val pipeBuildContext = newMockedPipeExecutionPlanBuilderContext
  val patternRel = PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)

  val planBuilder = new PipeExecutionPlanBuilder(monitors)

  def build(f: PlannerQuery => LogicalPlan): PipeInfo =
    planBuilder.build(f(solved))

  test("projection only query") {
    val logicalPlan = Projection(
      SingleRow(), Map("42" -> SignedDecimalIntegerLiteral("42") _))_
    val pipeInfo = build(logicalPlan)
    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(ProjectionNewPipe(SingleRowPipe(), Map("42" -> legacy.Literal(42)))())
  }

  test("simple pattern query") {
    val logicalPlan: AllNodesScan = AllNodesScan(IdName("n"), Set.empty)(solvedWithNodes("n"))
    val pipeInfo = planBuilder.build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(AllNodesScanPipe("n", RowSpec(nodes = Seq("n")), 0)())
  }

  test("simple label scan query") {
    val logicalPlan = NodeByLabelScan(IdName("n"), Right(LabelId(12)), Set.empty)(solvedWithNodes("n"))
    val pipeInfo = planBuilder.build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(
      NodeByLabelScanPipe("n", Right(LabelId(12)), RowSpec(nodes = Seq("n")), 0)())
  }

  test("simple node by id seek query") {
    val astLiteral: SignedIntegerLiteral = SignedDecimalIntegerLiteral("42")_
    val logicalPlan = NodeByIdSeek(IdName("n"), PlanEntityByIdExprs(Seq(astLiteral)), Set.empty)(solvedWithNodes("n"))
    val pipeInfo = planBuilder.build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(NodeByIdSeekPipe("n", PipeEntityByIdExprs(Seq(astLiteral.asCommandExpression)),
      RowSpec(nodes = Seq("n")), 0)())
  }

  test("simple node by id seek query with multiple values") {
    val astCollection: Collection = Collection(
      Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_)
    )_
    val logicalPlan = NodeByIdSeek(IdName("n"), PlanEntityByIdExprs(Seq(astCollection)), Set.empty)(solvedWithNodes("n"))
    val pipeInfo = planBuilder.build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(NodeByIdSeekPipe("n", PipeEntityByIdExprs(Seq(astCollection.asCommandExpression)),
      RowSpec(nodes = Seq("n")), 0)())
  }

  test("simple relationship by id seek query") {
    val astLiteral: SignedIntegerLiteral = SignedDecimalIntegerLiteral("42")_
    val fromNode = "from"
    val toNode = "to"
    val logicalPlan = DirectedRelationshipByIdSeek(
      IdName("r"), PlanEntityByIdExprs(Seq(astLiteral)), IdName(fromNode), IdName(toNode), Set.empty)_
    val pipeInfo = build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(DirectedRelationshipByIdSeekPipe("r",
      PipeEntityByIdExprs(Seq(astLiteral.asCommandExpression)), toNode, fromNode)())
  }

  test("simple relationship by id seek query with multiple values") {
    val astCollection: Seq[SignedIntegerLiteral] =
      Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_)

    val fromNode = "from"
    val toNode = "to"
    val logicalPlan = DirectedRelationshipByIdSeek(IdName("r"),
      PlanEntityByIdExprs(astCollection), IdName(fromNode), IdName(toNode), Set.empty)_
    val pipeInfo = build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(DirectedRelationshipByIdSeekPipe("r",
      PipeEntityByIdExprs(astCollection.map(_.asCommandExpression)), toNode, fromNode)())
  }

  test("simple undirected relationship by id seek query with multiple values") {
    val astCollection: Seq[SignedIntegerLiteral] =
      Seq(SignedDecimalIntegerLiteral("42")_, SignedDecimalIntegerLiteral("43")_, SignedDecimalIntegerLiteral("43")_)

    val fromNode = "from"
    val toNode = "to"
    val logicalPlan = UndirectedRelationshipByIdSeek(
      IdName("r"),
      PlanEntityByIdExprs(astCollection),
      IdName(fromNode),
      IdName(toNode),
      Set.empty)_
    val pipeInfo = build(logicalPlan)

    pipeInfo should not be 'updating
    pipeInfo.periodicCommit should equal(None)
    pipeInfo.pipe should equal(UndirectedRelationshipByIdSeekPipe("r",
      PipeEntityByIdExprs(astCollection.map(_.asCommandExpression)), toNode, fromNode)())
  }

  test("simple cartesian product") {
    val lhs = AllNodesScan(IdName("n"), Set.empty)(solvedWithNodes("n"))
    val rhs = AllNodesScan(IdName("m"), Set.empty)(solvedWithNodes("m"))
    val logicalPlan = CartesianProduct(lhs, rhs)_
    val pipeInfo = build(logicalPlan)

    pipeInfo.pipe should equal(CartesianProductPipe(
      AllNodesScanPipe("n", RowSpec(nodes = Seq("n")), 0)(),
      AllNodesScanPipe("m", RowSpec(nodes = Seq("m")), 0)())())
  }

  test("simple expand") {
    val allNodes = AllNodesScan("a", Set.empty)(solved)
    val solvedExpand = PlannerQuery(graph = QueryGraph(
      patternNodes = Set(IdName("a"), IdName("b")),
      patternRelationships = Set(PatternRelationship(IdName("r1"), (IdName("a"), IdName("b")), Direction.INCOMING, Seq(), SimplePatternLength))
    ))
    val logicalPlan = Expand(allNodes, "a", Direction.INCOMING, Direction.INCOMING, Seq(), "b", "r1", SimplePatternLength)(solvedExpand)
    val pipeInfo = planBuilder.build(logicalPlan)

    pipeInfo.pipe should equal(ExpandPipeForIntTypes(
      AllNodesScanPipe("a", RowSpec(nodes = Seq("a", "b"), relationships = Seq("r1")), 0)(),
      0, "a", 0, 1, "r1", "b", Direction.INCOMING, Seq() )())
  }

  test("simple hash join") {
    val solvedExpand1 = solvedWithPattern("a"->"r1"->"b")
    val solvedExpand2 = solvedWithPattern("c"->"r2"->"b")
    val solvedJoin = solvedExpand1 ++ solvedExpand2
    val logicalPlan =
      NodeHashJoin(
        Set(IdName("b")),
        Expand(AllNodesScan("a", Set.empty)(solved), "a", Direction.INCOMING, Direction.INCOMING, Seq(), "b", "r1", SimplePatternLength)(solvedExpand1),
        Expand(AllNodesScan("c", Set.empty)(solved), "c", Direction.INCOMING, Direction.INCOMING, Seq(), "b", "r2", SimplePatternLength)(solvedExpand2)
      )(solvedJoin)
    val pipeInfo = planBuilder.build(logicalPlan)

    pipeInfo.pipe should equal(NodeHashJoinPipe(
      Set("b"),
      ExpandPipeForIntTypes(
        AllNodesScanPipe("a", RowSpec(nodes = Seq("a", "b"), relationships = Seq("r1")), 0)(),
        0, "a", 0, 1, "r1", "b", Direction.INCOMING, Seq() )(),
      ExpandPipeForIntTypes(
        AllNodesScanPipe("c", RowSpec(nodes = Seq("c", "b"), relationships = Seq("r2")), 0)(),
        0, "c", 0, 1, "r2", "b", Direction.INCOMING, Seq() )()
    )())
  }

  test("use ExpandPipeForStringTypes when at least one is unknown") {
    val names = Seq("existing1", "nonexisting", "existing3")
    val relTypeNames = names.map(new RelTypeName(_)(null))
    val solvedExpand = PlannerQuery(graph = QueryGraph(
      patternNodes = Set(IdName("a"), IdName("b")),
      patternRelationships = Set(PatternRelationship(IdName("r1"), (IdName("a"), IdName("b")), Direction.INCOMING, Seq(), SimplePatternLength))
    ))

    val logicalPlan = Expand(
      AllNodesScan("a", Set.empty)(solved), "a", Direction.INCOMING, Direction.INCOMING, relTypeNames, "b", "r1", SimplePatternLength)(solvedExpand)
    val pipeInfo = planBuilder.build(logicalPlan)

    pipeInfo.pipe should equal(ExpandPipeForStringTypes(
      AllNodesScanPipe("a", RowSpec(nodes = Seq("a", "b"), relationships = Seq("r1")), 0)(),
      0, "a", 0, 1, "r1", "b", Direction.INCOMING, names)())
  }

  test("use ExpandPipeForIntTypes when all tokens are known") {
    val names = Seq("existing1", "existing2", "existing3")
    val relTypeNames = names.map(new RelTypeName(_)(null))
    val pattern: PlannerQuery = solvedWithPattern("a" -> "r1" -> "b")
    val logicalPlan = Expand(
      AllNodesScan("a", Set.empty)(solvedWithNodes("a")),
      "a", Direction.INCOMING, Direction.INCOMING, relTypeNames, "b", "r1", SimplePatternLength)(pattern)
    val pipeInfo = planBuilder.build(logicalPlan)

    pipeInfo.pipe should equal(ExpandPipeForIntTypes(
      AllNodesScanPipe("a", RowSpec(nodes = Seq("a", "b"), relationships = Seq("r1")), 0)(),
      0, "a", 0, 1, "r1", "b", Direction.INCOMING, Seq(1, 2, 3))())
  }

  test("use VarExpandPipeForStringTypes when at least one is unknown") {
    val names = Seq("existing1", "nonexisting", "existing3")
    val relTypeNames = names.map(new RelTypeName(_)(null))
    val logicalPlan = Expand(
      AllNodesScan("a", Set.empty)(solvedWithNodes("a")),
      "a", Direction.INCOMING, Direction.INCOMING, relTypeNames, "b", "r1", new VarPatternLength(2, Some(5)))(solvedWithPattern("a" -> "r1" -> "b"))
    val pipeInfo = planBuilder.build(logicalPlan)

    pipeInfo.pipe match {
      case pipe: VarLengthExpandPipeForStringTypes => pipe.copy(filteringStep = null)(pipe.estimatedCardinality) should equal(
        VarLengthExpandPipeForStringTypes(
          AllNodesScanPipe("a", RowSpec(nodes = Seq("a", "b"), relationships = Seq("r1")), 0)(),
          "a", "r1", "b", Direction.INCOMING, Direction.INCOMING, names, 2, Some(5))().copy(filteringStep = null)(pipe.estimatedCardinality))

      case _ => fail("expected VarLengthExpandPipeForStringTypes")
    }
  }

  test("use VarExpandPipeForIntTypes when all tokens are known") {
    val names = Seq("existing1", "existing2", "existing3")
    val relTypeNames = names.map(new RelTypeName(_)(null))
    val logicalPlan = Expand(AllNodesScan("a", Set.empty)(solved), "a", Direction.INCOMING, Direction.INCOMING,
      relTypeNames, "b", "r1", new VarPatternLength(2, Some(5)))(solvedWithPattern("a" -> "r1" -> "b"))
    val pipeInfo = planBuilder.build(logicalPlan)

    pipeInfo.pipe match {
      case pipe: VarLengthExpandPipeForIntTypes => pipe.copy(filteringStep = null)(pipe.estimatedCardinality) should equal(
        VarLengthExpandPipeForIntTypes(
          AllNodesScanPipe("a", RowSpec(nodes = Seq("a", "b"), relationships = Seq("r1")), 0)(),
          "a", "r1", "b", Direction.INCOMING, Direction.INCOMING, Seq(1, 2, 3), 2, Some(5))().copy(filteringStep = null)(pipe.estimatedCardinality))

      case _ => fail("expected VarLengthExpandPipeForIntTypes")
    }
  }
}
