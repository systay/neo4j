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
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class PlanEagernessTest extends CypherFunSuite with LogicalPlanConstructionTestSupport with AstConstructionTestSupport {

  private implicit var context: LogicalPlanningContext = null
  private var lpp: LogicalPlanProducer = null
  private val solved: PlannerQuery with CardinalityEstimation = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality.SINGLE)

  private def createNodeQG(name: String) = QueryGraph(mutatingPatterns = Seq(createNode(name)))
  private def createNodeQG(name: String, label: String) = QueryGraph(mutatingPatterns = Seq(createNode(name, Seq(label))))
  private def matchNode(name: String) = QueryGraph(patternNodes = Set(IdName(name)))
  private def matchNodeQG(name: String, label: String) = QueryGraph(patternNodes = Set(IdName(name))).withSelections(Selections.from(hasLabels(name, label)))
  private def matchRel(from: String, name: String, to: String, typ: Seq[String], dir: SemanticDirection) = {
    val fromId = IdName(from)
    val toId = IdName(to)
    val types = typ.map(x => RelTypeName(x)(pos))
    val rel = PatternRelationship(IdName(name), (fromId, toId), dir, types, SimplePatternLength)
    QueryGraph(patternNodes = Set(fromId, toId), patternRelationships = Set(rel))
  }

  private def argumentQG(name: String) = QueryGraph(argumentIds = Set(IdName(name)))
  private def mergeNodeQG(name: String, label: String) = {
    val readQG = matchNode(name) withPredicate hasLabels(name, label) withPredicate propEquality(name, "prop", 42)
    val createNodePattern = createNode(name, Seq(label)) andSetProperty("prop", 42)
    QueryGraph.empty withMutation MergeNodePattern(createNodePattern, readQG, Seq.empty, Seq.empty)
  }

  private def mergeNodeQG(name: String, label: String, prop: String, value: Int) = {
    val readQG = matchNode(name) withPredicate hasLabels(name, label) withPredicate propEquality(name, prop, value)
    val createNodePattern = createNode(name, Seq(label)) andSetProperty(prop, value)
    QueryGraph.empty withMutation MergeNodePattern(createNodePattern, readQG, Seq.empty, Seq.empty)
  }

  // Logical Plans
  private val allNodesScan = (name: String) => AllNodesScan(IdName(name), Set.empty)(CardinalityEstimation.lift(RegularPlannerQuery(QueryGraph(patternNodes = Set(IdName(name)))), Cardinality.SINGLE))
  private val argument = Argument(Set('x))(solved)(Map.empty)
  private val apply = Apply(argument, allNodesScan("a"))(solved)
  private val singleRow = SingleRow()(solved)
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
    val pq = RegularPlannerQuery(argumentQG("a"))
    val result = eagernessPlanner.apply(pq, lhs, head = false)

    result should equal(update(lhs))
  }

  test("overlapping MATCH and CREATE in a tail") {
    // given
    val lhs = allNodesScan("x")
    val pq = RegularPlannerQuery(createNodeQG("b") ++ matchNode("a"))

    // when
    val result = eagernessPlanner.apply(pq, lhs, head = false)

    // then
    result should equal(update(eager(lhs)))
  }

  test("overlapping MATCH and CREATE in head") {
    val lhs = allNodesScan("a")
    val pq = RegularPlannerQuery(createNodeQG("b") ++ matchNode("a"))
    val result = eagernessPlanner.apply(pq, lhs, head = true)

    result should equal(update(lhs))
  }

  test("overlapping MATCH with two nodes and CREATE in head") {
    // given
    val lhs = CartesianProduct(allNodesScan("a"), allNodesScan("b"))(null)
    val pq = RegularPlannerQuery(matchNode("a") ++ matchNode("b") ++ createNodeQG("b"))

    // when
    val result = eagernessPlanner.apply(pq, lhs, head = true)

    // then
    result should equal(update(eager(lhs)))
  }

  test("do not consider stable node ids when checking overlapping MATCH and CREATE in head") {
    // given
    val lhs = apply
    val pq = RegularPlannerQuery(argumentQG("a") ++ matchNode("b") ++ createNodeQG("b"))

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(eager(lhs)))
  }

  test("single node MERGE") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(lhs))
  }

  test("MERGE followed by CREATE on overlapping nodes") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))
    val tail = RegularPlannerQuery(createNodeQG(name = "b", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(eager(update(lhs)))
  }

  test("CREATE followed by MERGE on overlapping nodes") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(createNodeQG(name = "b", label = "A"))
    val tail = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(eager(update(lhs)))
  }

  test("MERGE followed by CREATE on not overlapping nodes") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))
    val tail = RegularPlannerQuery(createNodeQG(name = "b", label = "B"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("CREATE followed by MERGE on not overlapping nodes") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(createNodeQG(name = "b", label = "B"))
    val tail = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("MERGE followed by MATCH on overlapping nodes") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))
    val tail = RegularPlannerQuery(matchNodeQG(name = "b", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(eager(update(lhs)))
  }

  test("MATCH followed by MERGE on overlapping nodes") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(matchNodeQG(name = "b", label = "A"))
    val tail = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(update(eager(lhs)))
  }

  test("MERGE followed by MATCH on not overlapping nodes") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))
    val tail = RegularPlannerQuery(matchNodeQG(name = "b", label = "B"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("MATCH followed by MERGE on not overlapping nodes") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(matchNodeQG(name = "b", label = "B"))
    val tail = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("CREATE in head followed by MATCH") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(createNodeQG(name = "a"))
    val tail = RegularPlannerQuery(matchNode(name = "b"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = true)

    // then
    result should equal(update(lhs))
  }

  test("CREATE followed by MATCH on overlapping nodes with labels") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(createNodeQG(name = "a", label = "A"))
    val tail = RegularPlannerQuery(matchNodeQG(name = "b", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(eager(update(lhs)))
  }

  test("MATCH followed by CREATE on overlapping nodes with labels") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(matchNodeQG(name = "b", label = "A"))
    val tail = RegularPlannerQuery(createNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(update(eager(lhs)))
  }

  test("CREATE followed by MATCH on not overlapping nodes with labels") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(createNodeQG(name = "a", label = "A"))
    val tail = RegularPlannerQuery(matchNodeQG(name = "b", label = "B"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("MATCH followed by CREATE on not overlapping nodes with labels") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(matchNodeQG(name = "b", label = "B"))
    val tail = RegularPlannerQuery(createNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("MATCH with labels followed by CREATE without") {
    // given
    val lhs = singleRow
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
    val lhs = singleRow
    val readQG = matchNodeQG("a", "A")
    val pq = RegularPlannerQuery(readQG withMutation setLabel("a", "A"))

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("two MATCHes with labels followed by SET label") {
    // given
    val lhs = singleRow
    val qg = matchNodeQG("a", "A") ++ matchNode("b") withMutation setLabel("b", "A")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(eager(lhs)))
  }

  test("MATCH with property followed by SET property") {
    // given
    val lhs = singleRow
    val readQG = matchNode("a") withPredicate rewrittenPropEquality("a", "prop", 42)
    val pq = RegularPlannerQuery(readQG withMutation setNodeProperty("a", "prop"))

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("two MATCHes with property followed by SET property") {
    // given
    val lhs = singleRow
    val readQG = (matchNode("a") withPredicate rewrittenPropEquality("a", "prop", 42)) ++ matchNode("b")
    val pq = RegularPlannerQuery(readQG withMutation setNodeProperty("b", "prop"))

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(eager(lhs)))
  }

  test("Protect MATCH from REMOVE label") {
    // given

    val lhs = singleRow
    val firstQG = matchNodeQG("a", "A") ++ matchNode("b") withMutation removeLabel("b", "A")
    val pq = RegularPlannerQuery(firstQG)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(eager(lhs)))
  }

  test("When removing label from variable found through said label, no eagerness is needed") {
    // given

    val lhs = singleRow
    val firstQG = matchNodeQG("a", "A") withMutation removeLabel("a", "A")
    val pq = RegularPlannerQuery(firstQG)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("Matching on one label and removing another is OK") {
    // given

    val lhs = singleRow
    val firstQG = matchNode("a") ++
                  matchNodeQG("b", "B") withMutation removeLabel("b", "B")
    val pq = RegularPlannerQuery(firstQG)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("MATCH (n) CREATE (m) WITH * MATCH (o {prop:42}) SET n.prop2 = 42") {
    // given

    val lhs = allNodesScan("n")
    val firstQG = matchNode("n") withMutation createNode("m")
    val secondQG = matchNode("o") withPredicate propEquality("o","prop",42)  withMutation setNodeProperty("n", "prop2")
    val pq = RegularPlannerQuery(firstQG) withTail RegularPlannerQuery(secondQG)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(lhs))
  }

  test("MATCH (n) CREATE (m) in tail is not safe") {
    // given
    val lhs = allNodesScan("n")
    val firstQG = matchNode("n") withMutation createNode("m")
    val pq = RegularPlannerQuery(firstQG)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(eager(lhs)))
  }

  test("Protect MATCH from SET label") {
    // given

    val lhs = singleRow
    val firstQG = matchNodeQG("a", "A") ++ matchNode("b") withMutation setLabel("b", "A")
    val pq = RegularPlannerQuery(firstQG)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(eager(lhs)))
  }

  test("Protect OPTIONAL MATCH from SET label") {
    // given

    val lhs = singleRow

    val firstQG =  QueryGraph.empty.withAddedOptionalMatch(matchNodeQG("a", "A")) ++ matchNode("b") withMutation setLabel("b", "A")
    val pq = RegularPlannerQuery(firstQG)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(eager(lhs)))
  }

  test("match relationship without any updates") {
    // given
    val lhs = singleRow
    val qg = MATCH('a -> ('r :: 'T) -> 'c)
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("match relationship and then create relationship of same type") {
    // given
    val lhs = singleRow
    val qg = MATCH('a  -> ('r :: 'T) -> 'b) withMutation createRel('a -> ('r2 :: 'T) -> 'b)
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(eager(lhs)))
  }

  test("match relationship and then create relationship of same type, but with different properties") {
    // given
    val lhs = singleRow
    val read = MATCH('a -> ('r :: 'T) -> 'b) withPredicate propEquality("r", "prop", 42)
    val qg = read withMutation (createRel('a -> ('r2 :: 'T) -> 'b) andSetProperty("prop2", 42))
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("match relationship and then create relationship of different type needs no eager") {
    // given
    val lhs = singleRow
    val qg = MATCH('a  -> ('r :: 'T) -> 'b) withMutation createRel('a -> ('r2 :: 'T2) -> 'b)
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("match relationship and set properties on node from unknown map") {
    // given MATCH (a)-[r:T]->(b {prop: 42}) SET a = {param}
    val lhs = singleRow
    val readPart = MATCH('a -> ('r :: 'T) -> 'b) withPredicate propEquality("b", "prop", 42)
    val qg = readPart withMutation SetNodePropertiesFromMapPattern("a", Parameter("param")(pos), removeOtherProps = false)
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(eager(lhs)))
  }

  test("match relationship and set properties on node from known map") {
    // given MATCH (a)-[r:T]->(b {prop: 42}) SET a = {other: 42}
    val lhs = singleRow
    val readPart = MATCH('a -> ('r :: 'T) -> 'b) withPredicate propEquality("b", "prop", 42)

    val mapExpression = MapExpression(Seq((PropertyKeyName("other")(pos), literalInt(42))))(pos)
    val qg = readPart withMutation SetNodePropertiesFromMapPattern("a", mapExpression, removeOtherProps = false)
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(update(lhs))
  }

  test("the stable left-most leaf does not solve all predicates needed with map-expression") {
    // given MATCH (a {prop: 42})-[r:T]->(b) SET b += { prop: 42 }
    val predicate = propEquality("a", "prop", 42)
    val lhs = Selection(Seq(predicate), allNodesScan("a"))(solved)
    val readPart = MATCH('a -> ('r :: 'T) -> 'b) withPredicate predicate

    val mapExpression = MapExpression(Seq((PropertyKeyName("prop")(pos), literalInt(42))))(pos)
    val qg = readPart withMutation SetNodePropertiesFromMapPattern("b", mapExpression, removeOtherProps = false)
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(eager(lhs)))
  }

  test("the stable left-most leaf does not solve all predicates needed") {
    // given MATCH (a {prop: 42})-[r:T]->(b) SET b.prop += 42
    val predicate = propEquality("a", "prop", 42)
    val lhs = Selection(Seq(predicate), allNodesScan("a"))(solved)
    val readPart = MATCH('a -> ('r :: 'T) -> 'b) withPredicate predicate

    val qg = readPart withMutation setNodeProperty("b", "prop")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(eager(lhs)))
  }

  test("MATCH (a {prop : 5})-[r]-(b) SET b.prop2 = 5") {
    val lhs = singleRow
    val readPart = MATCH('a -- ('r :: 'T) -- 'b) withPredicate propEquality("a", "prop", 5)
    val qg = readPart withMutation setNodeProperty("b", "prop2")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(lhs))
  }

  test("Should not be eager when matching relationship and not writing to rels") {
    val lhs = singleRow
    val qg = MATCH('a -- 'r -- 'b) withMutation setNodeProperty("a", "prop")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(lhs))
  }

  test("Read relationship and update it in a way that does not fit the predicate on the relationship") {
    val lhs = singleRow
    val qg = MATCH('a -> 'r -> 'b) withMutation setRelProperty("r", "prop")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(lhs))
  }

  test("read two relationships, one with filter, set prop on other that matches filter") {
    val lhs = singleRow
    val read = MATCH('a -> 'r -> 'b) ++ MATCH('a2 -> 'r2 -> 'b2) withPredicate propEquality("r", "prop", 42)
    val qg = read withMutation setRelProperty("r2", "prop")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(eager(lhs)))
  }

  test("read two directed relationships, one with filter, set prop on same that matches filter") {
    val lhs = singleRow
    val read = MATCH('a -> 'r -> 'b) ++ MATCH('a2 -> 'r2 -> 'b2) withPredicate propEquality("r", "prop", 42)
    val qg = read withMutation setRelProperty("r", "prop")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(lhs))
  }

  test("when undirected, matching and setting needs eagerness") {
    val lhs = singleRow
    val read = MATCH('a -- 'r -- 'b) withPredicate propEquality("r", "prop", 42)
    val qg = read withMutation setRelProperty("r", "prop")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(eager(lhs)))
  }

  test("when directed, matching and setting does not need eagerness") {
    val lhs = singleRow
    val read = MATCH('a -> 'r -> 'b) withPredicate propEquality("r", "prop", 42)
    val qg = read withMutation setRelProperty("r", "prop")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(lhs))
  }

  test("when directed, matching and deleting does not need eagerness") {
    // given
    val lhs = singleRow
    val read = MATCH('a -> 'r -> 'b)
    val qg = read withMutation delete("r")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(lhs))
  }

  test("when undirected, matching and deleting needs eagerness") {
    // given
    val lhs = singleRow
    val read = MATCH('a -- 'r -- 'b)
    val qg = read withMutation delete("a") withMutation delete("r") withMutation delete("b")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(eager(lhs)))
  }

  test("undirected relationship, and we are creating new matching relationships with matching type and props") {
    // given
    val lhs = singleRow
    val read = MATCH('a -> ('r :: 'T) -> 'b) withPredicate propEquality("r", "prop", 43)
    val qg = read withMutation (createRel('a -> ('r2 :: 'T) ->'b) andSetProperty ("prop", 43))
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(eager(lhs)))
  }

  test("match with unstable leaf and then delete needs eagerness") {
    // given
    val lhs = singleRow
    val read = matchNodeQG("a", "A") ++ matchNodeQG("b", "B")
    val qg = read withMutation delete("a") withMutation delete("b")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(eager(lhs)))
  }

  test("match relationship and delete path") {
    // given
    val lhs = singleRow
    val read = MATCH('a -> 'r -> 'r)
    val qg = read withMutation delete('a -> 'r -> 'r)
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(eager(lhs)))
  }

  test("match, delete and then merge a node") {
    // given
    val lhs = singleRow
    val firstQ = matchNodeQG("b", "B") withMutation delete("b")
    val secondQG = mergeNodeQG("c", "B", "prop", 42)
    val pq = RegularPlannerQuery(firstQ) withTail RegularPlannerQuery(secondQG)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(eager(update(eager(lhs))))
  }

  test("match relationship and then create single node") {
    // given
    val lhs = singleRow
    val qg = MATCH('a -> 'r -> 'b) withMutation createNode("c")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(lhs))
  }

  test("match relationship and then create single node") {
    // given
    val lhs = singleRow
    val firstQG = MATCH('a -> 'r -> 'b) withMutation delete("r")
    val secondQG = merge('a -> 'r -> 'b)
    val pq = RegularPlannerQuery(firstQG) withTail RegularPlannerQuery(secondQG)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(update(lhs))
  }

  implicit class richRel(r: CreateRelationshipPattern) {
    def andSetProperty(propName: String, intVal: Int): CreateRelationshipPattern =
      r.copy(properties = Some(MapExpression(Seq((PropertyKeyName(propName)(pos), literalInt(intVal))))(pos)))
  }
  implicit class richNode(r: CreateNodePattern) {
    def andSetProperty(propName: String, intVal: Int): CreateNodePattern =
      r.copy(properties = Some(MapExpression(Seq((PropertyKeyName(propName)(pos), literalInt(intVal))))(pos)))
  }

  implicit class qgHelper(qg: QueryGraph) {
    def withPredicate(e: Expression): QueryGraph = qg.withSelections(qg.selections ++ Selections.from(e))

    def withMutation(patterns: MutatingPattern*): QueryGraph = qg.addMutatingPatterns(patterns: _*)
  }

  implicit private class crazyDslStart(in: Symbol) {
    def ->(rel: VarAndType) = new LeftNodeAndRel(VarWithoutType(in), rel)
    def --(rel: VarAndType) = new LeftNodeAndRel(VarWithoutType(in), rel)
    def -<-(rel: VarAndType) = new LeftNodeAndRel(VarWithoutType(in), rel)

    def ->(rel: Symbol) = new LeftNodeAndRel(VarWithoutType(in), VarWithoutType(rel))
    def --(rel: Symbol) = new LeftNodeAndRel(VarWithoutType(in), VarWithoutType(rel))
    def -<-(rel: Symbol) = new LeftNodeAndRel(VarWithoutType(in), VarWithoutType(rel))

    def ::(other: Symbol) = new VarAndType(other, in)
  }

  private sealed case class LeftNodeAndRel(from: Var, rel: Var) {
    def -> (to: Symbol) = new Pattern(from, rel, VarWithoutType(to), SemanticDirection.OUTGOING)
    def -- (to: Symbol) = new Pattern(from, rel, VarWithoutType(to), SemanticDirection.BOTH)
    def -<- (to: Symbol) = new Pattern(from, rel, VarWithoutType(to), SemanticDirection.INCOMING)
  }

  trait Var {
    def variableName: Symbol
  }
  private sealed case class VarAndType(variableName: Symbol, relType: Symbol) extends Var {
    def ->(rel: VarAndType) = new LeftNodeAndRel(rel, this)
  }
  private sealed case class VarWithoutType(variableName: Symbol) extends Var {
    def ->(rel: VarAndType) = new LeftNodeAndRel(this, rel)
  }

  private sealed case class Pattern(from: Var, rel: Var, to: Var, dir: SemanticDirection)

  // capitalized because `match` is a Scala keyword
  private def MATCH(pattern: Pattern): QueryGraph = {
    matchRel(pattern.from.variableName.name, pattern.rel.variableName.name, pattern.to.variableName.name, readTypes(pattern), pattern.dir)
  }

  private def merge(pattern: Pattern): QueryGraph = {
    val readQG = MATCH(pattern)
    val from = IdName(pattern.from.variableName.name)
    val r = IdName(pattern.rel.variableName.name)
    val to = IdName(pattern.to.variableName.name)
    val typ = RelTypeName(pattern.rel.asInstanceOf[VarAndType].relType.name)(pos)
    val relationshipPattern = CreateRelationshipPattern(r, from, typ, to, None, SemanticDirection.OUTGOING)
    val fromP = createNode(from.name)
    val toP = createNode(to.name)
    QueryGraph.empty withMutation MergeRelationshipPattern(Seq(fromP, toP), Seq(relationshipPattern), readQG, Seq.empty, Seq.empty)
  }

  private def readTypes(xx: Pattern) = xx.rel match {
    case VarAndType(_, typ) => Seq(typ.name)
    case VarWithoutType(_) => Seq.empty
  }
  private def writeType(xx: Pattern) = xx.rel match {
    case VarAndType(_, typ) => RelTypeName(typ.name)(pos)
    case VarWithoutType(_) => ???
  }

  private def createRel(pattern: Pattern) =
    CreateRelationshipPattern(pattern.rel.variableName, pattern.from.variableName, writeType(pattern), pattern.to.variableName, None, SemanticDirection.OUTGOING)

  private def eager(inner: LogicalPlan) = Eager(inner)(solved)

  private def createNode(name: String, labels: Seq[String] = Seq.empty): CreateNodePattern =
    CreateNodePattern(IdName(name), labels.map(x => LabelName(x)(pos)), None)
  private def setLabel(name: String, labels: String*): SetLabelPattern =
    SetLabelPattern(IdName(name), labels.map(x => LabelName(x)(pos)))
  private def removeLabel(name: String, labels: String*): RemoveLabelPattern =
    RemoveLabelPattern(IdName(name), labels.map(x => LabelName(x)(pos)))
  private def setNodeProperty(name: String, propKey: String): SetNodePropertyPattern =
    SetNodePropertyPattern(IdName(name), PropertyKeyName(propKey)(pos), StringLiteral("new property value")(pos))
  private def setRelProperty(name: String, propKey: String) =
    SetRelationshipPropertyPattern(IdName(name), PropertyKeyName(propKey)(pos), StringLiteral("new property value")(pos))
  private def delete(variable: String) =
    DeleteExpressionPattern(varFor(variable), forced = false)

  private def delete(pattern: Pattern) = {
    val from = varFor(pattern.from.variableName.name)
    val r = varFor(pattern.rel.variableName.name)
    val to = varFor(pattern.to.variableName.name)
    val path = NodePathStep(from, SingleRelationshipPathStep(r, SemanticDirection.OUTGOING, NodePathStep(to, NilPathStep)))

    DeleteExpressionPattern(PathExpression(path)(pos), forced = false)
  }
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
