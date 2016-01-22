/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v3_0.planner._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class PlanEagernessTest extends CypherFunSuite with LogicalPlanConstructionTestSupport with AstConstructionTestSupport {

  private implicit var context: LogicalPlanningContext = null
  private var lpp: LogicalPlanProducer = null
  private val solved: PlannerQuery with CardinalityEstimation = null


  private def createNodeQG(name: String) = QueryGraph(mutatingPatterns = Seq(createNode(name)))
  private def createNodeQG(name: String, label: String) = QueryGraph(mutatingPatterns = Seq(createNode(name, Seq(label))))
  private def matchNodeQG(name: String) = QueryGraph(patternNodes = Set(IdName(name)))
  private def matchNodeQG(name: String, label: String) = QueryGraph(patternNodes = Set(IdName(name))).withSelections(Selections.from(hasLabels(name, label)))
  private val argumentQG = QueryGraph(argumentIds = Set('a))
  private def mergeNodeQG(name: String, label: String) = {
    val readQG: QueryGraph = matchNodeQG(name).withSelections(Selections.from(hasLabels(name, label)))
    QueryGraph(
      mutatingPatterns = Seq(MergeNodePattern(createNode(name, Seq(label)), readQG, Seq.empty, Seq.empty)))
  }

  // PLANS
  private val allNodesScan = (name: String) => AllNodesScan(IdName(name), Set.empty)(solved)
  private val argument = Argument(Set('x))(solved)(Map.empty)
  private val apply = Apply(argument, allNodesScan("a"))(solved)

  private val innerUpdatePlanner: FakePlanner = spy(FakePlanner())
  private val eagernessPlanner: PlanEagerness = PlanEagerness(innerUpdatePlanner)

  override protected def initTest(): Unit = {
    super.initTest()
    context = mock[LogicalPlanningContext]
    lpp = mock[LogicalPlanProducer]
    when(context.logicalPlanProducer).thenReturn(lpp)
    when(lpp.planEager(any())).thenAnswer(new Answer[LogicalPlan] {
      override def answer(invocation: InvocationOnMock): LogicalPlan =
        eager(invocation.getArguments.apply(0).asInstanceOf[LogicalPlan])
    })
    reset(innerUpdatePlanner)
  }

  test("MATCH only") {
    val lhs = allNodesScan("a")
    val pq = RegularPlannerQuery(argumentQG)
    val result = eagernessPlanner.apply(pq, lhs, head = false)

    result should equal(update(lhs))
    verify(innerUpdatePlanner).apply(any(), any(), any())(any())
  }

  test("overlapping MATCH and CREATE in a tail") {
    // given
    val lhs = allNodesScan("x")
    val pq = RegularPlannerQuery(createNodeQG("b") ++ matchNodeQG("a"))

    // when
    val result = eagernessPlanner.apply(pq, lhs, head = false)

    // then
    result should equal(update(eager(lhs)))
  }

  test("overlapping MATCH and CREATE in head") {
    val lhs = allNodesScan("a")
    val pq = RegularPlannerQuery(createNodeQG("b") ++ matchNodeQG("a"))
    val result = eagernessPlanner.apply(pq, lhs, head = true)

    result should equal(update(lhs))
  }

  test("overlapping MATCH with two nodes and CREATE in head") {
    // given
    val lhs = CartesianProduct(allNodesScan("a"), allNodesScan("b"))(null)
    val pq = RegularPlannerQuery(matchNodeQG("a") ++ matchNodeQG("b") ++ createNodeQG("b"))

    // when
    val result = eagernessPlanner.apply(pq, lhs, head = true)

    // then
    result should equal(update(eager(lhs)))
  }

  test("do not consider stable node ids when checking overlapping MATCH and CREATE in head") {
    // given
    val lhs = apply
    val pq = RegularPlannerQuery(argumentQG ++ matchNodeQG("b") ++ createNodeQG("b"))

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(eager(apply)))
  }

  test("single node MERGE") {
    // given
    val lhs = SingleRow()(solved)
    val pq = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(lhs))
  }

  test("MERGE followed by CREATE on overlapping nodes") {
    // given
    val lhs = SingleRow()(solved)
    val pq = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))
    val tail = RegularPlannerQuery(createNodeQG(name = "b", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(eager(update(lhs)))
  }

  test("CREATE followed by MERGE on overlapping nodes") {
    // given
    val lhs = SingleRow()(solved)
    val pq = RegularPlannerQuery(createNodeQG(name = "b", label = "A"))
    val tail = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(eager(update(lhs)))
  }

  test("MERGE followed by CREATE on not overlapping nodes") {
    // given
    val lhs = SingleRow()(solved)
    val pq = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))
    val tail = RegularPlannerQuery(createNodeQG(name = "b", label = "B"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("CREATE followed by MERGE on not overlapping nodes") {
    // given
    val lhs = SingleRow()(solved)
    val pq = RegularPlannerQuery(createNodeQG(name = "b", label = "B"))
    val tail = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("MERGE followed by MATCH on overlapping nodes") {
    // given
    val lhs = SingleRow()(solved)
    val pq = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))
    val tail = RegularPlannerQuery(matchNodeQG(name = "b", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(eager(update(lhs)))
  }

  test("MATCH followed by MERGE on overlapping nodes") {
    // given
    val lhs = SingleRow()(solved)
    val pq = RegularPlannerQuery(matchNodeQG(name = "b", label = "A"))
    val tail = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(update(eager(lhs)))
  }

  test("MERGE followed by MATCH on not overlapping nodes") {
    // given
    val lhs = SingleRow()(solved)
    val pq = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))
    val tail = RegularPlannerQuery(matchNodeQG(name = "b", label = "B"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("MATCH followed by MERGE on not overlapping nodes") {
    // given
    val lhs = SingleRow()(solved)
    val pq = RegularPlannerQuery(matchNodeQG(name = "b", label = "B"))
    val tail = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("CREATE in head followed by MATCH") {
    // given
    val lhs = SingleRow()(solved)
    val pq = RegularPlannerQuery(createNodeQG(name = "a"))
    val tail = RegularPlannerQuery(matchNodeQG(name = "b"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = true)

    // then
    result should equal(update(lhs))
  }

  test("CREATE followed by MATCH on overlapping nodes with labels") {
    // given
    val lhs = SingleRow()(solved)
    val pq = RegularPlannerQuery(createNodeQG(name = "a", label = "A"))
    val tail = RegularPlannerQuery(matchNodeQG(name = "b", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(eager(update(lhs)))
  }

  test("MATCH followed by CREATE on overlapping nodes with labels") {
    // given
    val lhs = SingleRow()(solved)
    val pq = RegularPlannerQuery(matchNodeQG(name = "b", label = "A"))
    val tail = RegularPlannerQuery(createNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(update(eager(lhs)))
  }

  test("CREATE followed by MATCH on not overlapping nodes with labels") {
    // given
    val lhs = SingleRow()(solved)
    val pq = RegularPlannerQuery(createNodeQG(name = "a", label = "A"))
    val tail = RegularPlannerQuery(matchNodeQG(name = "b", label = "B"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("MATCH followed by CREATE on not overlapping nodes with labels") {
    // given
    val lhs = SingleRow()(solved)
    val pq = RegularPlannerQuery(matchNodeQG(name = "b", label = "B"))
    val tail = RegularPlannerQuery(createNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("MATCH with labels followed by CREATE without") {
    // given
    val lhs = SingleRow()(solved)
    val readQG = matchNodeQG("a", "A")
    val qg = readQG ++ createNodeQG("b")
    val pq = RegularPlannerQuery(qg ++ createNodeQG("b"))

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("MATCH with labels followed by SET label") {
    // given
    val lhs = SingleRow()(solved)
    val readQG = matchNodeQG("a", "A")
    val pq = RegularPlannerQuery(readQG withMutation setLabel("a", "A"))

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("two MATCHes with labels followed by SET label") {
    // given
    val lhs = SingleRow()(solved)
    val qg = matchNodeQG("a", "A") ++ matchNodeQG("b") withMutation setLabel("b", "A")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(eager(lhs)))
  }

  test("MATCH with property followed by SET property") {
    // given
    val lhs = SingleRow()(solved)
    val readQG = matchNodeQG("a") withPredicate rewrittenPropEquality("a", "prop", 42)
    val pq = RegularPlannerQuery(readQG withMutation setProperty("a", "prop"))

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(eager(lhs)))
  }

  test("two MATCHes with property followed by SET property") {
    // given
    val lhs = SingleRow()(solved)
    val readQG = (matchNodeQG("a") withPredicate rewrittenPropEquality("a", "prop", 42)) ++ matchNodeQG("b")
    val pq = RegularPlannerQuery(readQG withMutation setProperty("b", "prop"))

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(eager(lhs)))
  }

  implicit class qgHelper(qg: QueryGraph) {
    def withPredicate(e: Expression): QueryGraph = qg.withSelections(qg.selections ++ Selections.from(e))

    def withMutation(patterns: MutatingPattern*): QueryGraph = qg.addMutatingPatterns(patterns: _*)
  }

  private def eager(inner: LogicalPlan) = Eager(inner)(solved)
  private def createNode(name: String, labels: Seq[String] = Seq.empty): CreateNodePattern =
    CreateNodePattern(IdName(name), labels.map(x => LabelName(x)(pos)), None)
  private def setLabel(name: String, labels: String*): SetLabelPattern =
    SetLabelPattern(IdName(name), labels.map(x => LabelName(x)(pos)))
  private def setProperty(name: String, propKey: String): SetNodePropertyPattern =
    SetNodePropertyPattern(IdName(name), PropertyKeyName(propKey)(pos), StringLiteral("new property value")(pos))
}

case class update(inner: LogicalPlan) extends LogicalPlan {
  override def lhs = Some(inner)
  override def rhs = None

  override def solved: PlannerQuery with CardinalityEstimation = ???

  override def availableSymbols: Set[IdName] = ???

  override def mapExpressions(f: (Set[IdName], Expression) => Expression): LogicalPlan = ???

  override def strictness: StrictnessMode = ???
}

case class FakePlanner() extends LogicalPlanningFunction3[PlannerQuery, LogicalPlan, Boolean, LogicalPlan] {
  override def apply(plannerQuery: PlannerQuery, lhs: LogicalPlan, head: Boolean)
                    (implicit context: LogicalPlanningContext): LogicalPlan = update(lhs)
}
