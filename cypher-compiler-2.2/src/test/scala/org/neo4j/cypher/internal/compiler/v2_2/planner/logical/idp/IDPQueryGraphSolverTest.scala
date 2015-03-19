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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.idp

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Cardinality
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanningTestSupport2, QueryGraph, Selections}
import org.neo4j.graphdb.Direction

import scala.collection.immutable

class IDPQueryGraphSolverTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should plan for a single node pattern") {
    new given {
      queryGraphSolver = IDPQueryGraphSolver(solvers = Seq.empty)
      qg = QueryGraph(patternNodes = Set("a"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        AllNodesScan("a", Set.empty)(null)
      )
    }
  }

  test("should plan cartesian product between 3 pattern nodes") {
    new given {
      queryGraphSolver = IDPQueryGraphSolver(solvers = Seq.empty)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c")
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        CartesianProduct(
          AllNodesScan("a", Set.empty)(null),
          CartesianProduct(
            AllNodesScan("b", Set.empty)(null),
            AllNodesScan("c", Set.empty)(null)
          )(null)
        )(null)
      )
    }
  }

  test("should plan for a single relationship pattern") {
    new given {
      queryGraphSolver = IDPQueryGraphSolver(solvers = Seq.empty)
      qg = QueryGraph(
        patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)),
        selections = Selections.from(HasLabels(ident("b"), Seq(LabelName("B")(pos)))(pos))
      )

      labelCardinality = immutable.Map(
        "B" -> Cardinality(10)
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        Expand(NodeByLabelScan("b", LazyLabel("B"), Set.empty)(null), "b", Direction.INCOMING, Seq.empty, "a", "r")(null)
      )
    }
  }

  test("should plan for a single relationship pattern with labels on both sides") {
    val labelBPredicate = HasLabels(ident("b"), Seq(LabelName("B")(pos)))(pos)
    new given {
      queryGraphSolver = IDPQueryGraphSolver(solvers = Seq.empty)
      qg = QueryGraph(
        patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)),
        selections = Selections.from(
          HasLabels(ident("a"), Seq(LabelName("A")(pos)))(pos),
          labelBPredicate)
      )

      labelCardinality = immutable.Map(
        "A" -> Cardinality(10),
        "B" -> Cardinality(1000)
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        Selection(Seq(labelBPredicate),
          Expand(
            NodeByLabelScan("a", LazyLabel("A"), Set.empty)(null), "a", Direction.OUTGOING, Seq.empty, "b", "r")(null)
        )(null))
    }
  }

  test("should plan for a join between two pattern relationships") {
    // MATCH (a:A)-[r1]->(c)-[r2]->(b:B)
    new given {
      queryGraphSolver = IDPQueryGraphSolver(solvers = Seq(joinSolverStep(_)))
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        patternRelationships = Set(
          PatternRelationship("r1", ("a", "c"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("c", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
        ),
        selections = Selections.from(
          HasLabels(ident("a"), Seq(LabelName("A")(pos)))(pos),
          HasLabels(ident("b"), Seq(LabelName("B")(pos)))(pos))
      )

      labelCardinality = immutable.Map(
        "A" -> Cardinality(10),
        "B" -> Cardinality(10)
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        NodeHashJoin(Set("c"),
          Expand(
            NodeByLabelScan("a", LazyLabel("A"), Set.empty)(null), "a", Direction.OUTGOING, Seq.empty, "c", "r1")(null),
          Expand(
            NodeByLabelScan("b", LazyLabel("B"), Set.empty)(null), "b", Direction.INCOMING, Seq.empty, "c", "r2")(null)
        )(null))
    }
  }

  test("should plan for a join between two pattern relationships and apply a selection") {
    // MATCH (a:A)-[r1]->(c)-[r2]->(b:B) WHERE r1.foo = r2.foo
    new given {
      val predicate = Equals(Property(ident("r1"), PropertyKeyName("foo")(pos))(pos), Property(ident("r2"), PropertyKeyName("foo")(pos))(pos))(pos)
      queryGraphSolver = IDPQueryGraphSolver(solvers = Seq(joinSolverStep(_)))
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        patternRelationships = Set(
          PatternRelationship("r1", ("a", "c"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("c", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
        ),
        selections = Selections.from(
          predicate,
          HasLabels(ident("a"), Seq(LabelName("A")(pos)))(pos),
          HasLabels(ident("b"), Seq(LabelName("B")(pos)))(pos))
      )

      labelCardinality = immutable.Map(
        "A" -> Cardinality(10),
        "B" -> Cardinality(10)
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        Selection(Seq(cfg.predicate),
          NodeHashJoin(Set("c"),
            Expand(
              NodeByLabelScan("a", LazyLabel("A"), Set.empty)(null), "a", Direction.OUTGOING, Seq.empty, "c", "r1")(null),
            Expand(
              NodeByLabelScan("b", LazyLabel("B"), Set.empty)(null), "b", Direction.INCOMING, Seq.empty, "c", "r2")(null)
          )(null)
        )(null)
      )
    }
  }

  test("should solve self looping pattern") {
    new given {
      queryGraphSolver = IDPQueryGraphSolver(solvers = Seq.empty)
      qg = QueryGraph(
        patternNodes = Set("a"),
        patternRelationships = Set(PatternRelationship("r", ("a", "a"), Direction.OUTGOING, Seq.empty, SimplePatternLength))
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        Expand(AllNodesScan("a", Set.empty)(null), "a", Direction.OUTGOING, Seq.empty, "a", IdName("r"), ExpandInto)(null)
      )
    }
  }

  test("should solve double expand") {
    new given {
      queryGraphSolver = IDPQueryGraphSolver(solvers = Seq(expandSolverStep(_)))
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        patternRelationships = Set(
          PatternRelationship("r1", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("b", "c"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
        )
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        Expand(
          Expand(
            AllNodesScan("b", Set.empty)(null),
            "b", Direction.OUTGOING, Seq.empty, "c", IdName("r2"), ExpandAll
          )(null),
          "b", Direction.INCOMING, Seq.empty, "a", IdName("r1"), ExpandAll
        )(null)
      )
    }
  }

  test("should solve empty graph with SingleRow") {
    new given {
      queryGraphSolver = IDPQueryGraphSolver(solvers = Seq.empty)
      qg = QueryGraph.empty
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        SingleRow()(solved)
      )
    }
  }

  test("should plan a simple argument row when everything is covered") {
    new given {
      queryGraphSolver = IDPQueryGraphSolver(solvers = Seq.empty)
      qg = QueryGraph(argumentIds = Set("a"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        Argument(Set("a"))(null)()
      )
    }
  }

  test("should handle projected endpoints") {
    new given {
      queryGraphSolver = IDPQueryGraphSolver(solvers = Seq.empty)
      qg = QueryGraph(
        patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)),
        argumentIds = Set("r"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        ProjectEndpoints(Argument(Set("r"))(null)(), "r", "a", startInScope = false, "b", endInScope = false, None, directed = true, SimplePatternLength)(null)
      )
    }
  }

  test("should expand from projected endpoints") {
    new given {
      queryGraphSolver = IDPQueryGraphSolver(solvers = Seq(expandSolverStep(_)))
      val pattern1 = PatternRelationship("r1", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
      val pattern2 = PatternRelationship("r2", ("b", "c"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        patternRelationships = Set(pattern1, pattern2),
        argumentIds = Set("r1"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        Expand(
          ProjectEndpoints(Argument(Set("r1"))(null)(), "r1", "a", startInScope = false, "b", endInScope = false, None, directed = true, SimplePatternLength)(null),
          "b", Direction.OUTGOING, Seq.empty, "c", "r2", ExpandAll
        )(null)
      )
    }
  }

  test("should plan a relationship pattern based on an argument row since part of the node pattern is already solved") {
    new given {
      queryGraphSolver = IDPQueryGraphSolver(solvers = Seq(expandSolverStep(_)))
      qg = QueryGraph(patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)),
        argumentIds = Set("a")
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        Expand(Argument(Set("a"))(null)(), "a", Direction.OUTGOING, Seq.empty, "b", "r", ExpandAll)(null)
      )
    }
  }

  test("should plan big star pattern") {
    // keep around, practical for investigating performance
    val numberOfPatternRelationships = 10

    new given {
      queryGraphSolver = IDPQueryGraphSolver()
      val patternRels = for (i <- 1 to numberOfPatternRelationships) yield {
        PatternRelationship("r" + i, ("n" + i, "x"), Direction.INCOMING, Seq.empty, SimplePatternLength)
      }

      val patternNodes = for (i <- 1 to numberOfPatternRelationships) yield {
        IdName("n" + i)
      }

      qg = QueryGraph(patternNodes = patternNodes.toSet + IdName("x"), patternRelationships = patternRels.toSet)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx
      queryGraphSolver.plan(cfg.qg) // should not throw
    }
  }

  test("should plan big chain pattern") {
    // keep around, practical for investigating performance
    val numberOfPatternRelationships = 10

    new given {
      val patternRels = for (i <- 1 to numberOfPatternRelationships-1) yield {
        PatternRelationship("r" + i, ("n" + i, "n" + (i+1)), Direction.INCOMING, Seq.empty, SimplePatternLength)
      }

      val patternNodes = for (i <- 1 to numberOfPatternRelationships) yield {
        IdName("n" + i)
      }

      queryGraphSolver = IDPQueryGraphSolver()
      qg = QueryGraph(patternNodes = patternNodes.toSet, patternRelationships = patternRels.toSet)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx
      queryGraphSolver.plan(cfg.qg) // should not throw
    }
  }

  test("should solve planning an empty QG with arguments") {
    new given {
      queryGraphSolver = IDPQueryGraphSolver(solvers = Seq.empty)
      qg = QueryGraph(argumentIds = Set("a"), patternNodes = Set("a"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        Argument(Set("a"))(null)()
      )
    }
  }

  test("should plan cartesian product between 3 pattern nodes and using a single predicate between 2 pattern nodes") {
    new given {
      queryGraphSolver = IDPQueryGraphSolver(solvers = Seq.empty)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        selections = Selections.from(Equals(ident("b"), ident("c"))(pos)))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        Selection(cfg.qg.selections.flatPredicates,
          CartesianProduct(
            AllNodesScan("b", Set.empty)(null),
            CartesianProduct(
              AllNodesScan("a", Set.empty)(null),
              AllNodesScan("c", Set.empty)(null)
            )(null)
          )(null)
        )(null)
      )
    }
  }

  test("should plan cartesian product between 1 pattern nodes and 1 pattern relationship") {
    new given {
      queryGraphSolver = IDPQueryGraphSolver(solvers = Seq.empty)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        selections = Selections.from(Equals(ident("b"), ident("c"))(pos)),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength))
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        Selection(cfg.qg.selections.flatPredicates,
          CartesianProduct(
            AllNodesScan("c", Set.empty)(null),
            Expand(AllNodesScan("b", Set.empty)(null), "b", Direction.INCOMING, Seq.empty, "a", "r", ExpandAll)(null)
          )(null)
        )(null)
      )
    }
  }

  test("should plan for optional single relationship pattern") {
    new given {
      queryGraphSolver = IDPQueryGraphSolver()
      qg = QueryGraph(// MATCH a OPTIONAL MATCH a-[r]->b
        patternNodes = Set("a"),
        optionalMatches = Seq(QueryGraph(
          patternNodes = Set("a", "b"),
          argumentIds = Set("a"),
          patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength))
        ))
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        Apply(
          AllNodesScan("a", Set.empty)(solved),
          Optional(
            Expand(Argument(Set("a"))(solved)(), "a", Direction.OUTGOING, Seq.empty, "b", "r")(solved)
          )(solved)
        )(solved)
      )
    }
  }

  test("should plan for optional single relationship pattern between two known nodes") {
    new given {
      queryGraphSolver = IDPQueryGraphSolver(solvers = Seq.empty)
      qg = QueryGraph(// MATCH a, b OPTIONAL MATCH a-[r]->b
        patternNodes = Set("a", "b"),
        optionalMatches = Seq(QueryGraph(
          patternNodes = Set("a", "b"),
          argumentIds = Set("a", "b"),
          patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength))
        ))
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        OuterHashJoin(
          Set("a", "b"),
          CartesianProduct(
            AllNodesScan(IdName("a"), Set.empty)(null),
            AllNodesScan(IdName("b"), Set.empty)(null)
          )(null),
          Expand(
            AllNodesScan(IdName("b"), Set.empty)(null),
            "b", Direction.INCOMING, Seq.empty, "a", "r", ExpandAll
          )(null)
        )(null)
      )
    }
  }

  test("should handle query starting with an optional match") {
    new given {
      queryGraphSolver = IDPQueryGraphSolver(solvers = Seq.empty)
      qg = QueryGraph( // OPTIONAL MATCH a-->b RETURN b a
        patternNodes = Set.empty,
        argumentIds = Set.empty,
        optionalMatches = Seq(QueryGraph(
          patternNodes = Set("a", "b"),
          argumentIds = Set.empty,
          patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)))
        )
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        Apply(
          SingleRow()(solved),
          Optional(
            Expand(
              AllNodesScan("b", Set.empty)(null),
              "b", Direction.INCOMING, Seq.empty, "a", "r", ExpandAll
            )(solved)
          )(solved)
        )(solved)
      )
    }
  }

  test("should handle relationship by id") {
    new given {
      queryGraphSolver = IDPQueryGraphSolver(solvers = Seq.empty)
      qg = QueryGraph( // MATCH (a)-[r]->(b) WHERE id(r) = 42 RETURN *
        patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)),
        selections = Selections.from(In(FunctionInvocation(FunctionName("id")(pos), ident("r"))(pos), Collection(Seq(SignedDecimalIntegerLiteral("42")(pos)))(pos))(pos))
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        DirectedRelationshipByIdSeek("r", EntityByIdExprs(Seq(SignedDecimalIntegerLiteral("42")(pos))), "a", "b", Set.empty)(null)
      )
    }
  }

  test("should handle multiple project end points on arguments when creating leaf plans") {
    new given {
      queryGraphSolver = IDPQueryGraphSolver()
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        patternRelationships = Set(
          PatternRelationship("r1", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("b", "c"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
        ),
        argumentIds = Set("r1", "r2")
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        ProjectEndpoints(
          ProjectEndpoints(
            Argument(Set("r1", "r2"))(null)(),
            "r2", "b", startInScope = false, "c", endInScope = false, None, directed = true, SimplePatternLength)(null),
          "r1", "a", startInScope = false, "b", endInScope = true, None, directed = true, SimplePatternLength)(null))
    }
  }

  test("should handle passing multiple projectible relationships as arguments") {
    new given {
      queryGraphSolver = IDPQueryGraphSolver(solvers = Seq(expandSolverStep(_)))
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c", "d"),
        patternRelationships = Set(
          PatternRelationship("r1", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("c", "d"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r3", ("a", "d"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
        ),
        argumentIds = Set("a", "b", "c", "d", "r1", "r2")
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        ProjectEndpoints(
          Expand(
            ProjectEndpoints(
              Argument(Set("a", "b", "c", "d", "r1", "r2"))(null)(),
              "r2", "c", startInScope = true, "d", endInScope = true, None, directed = true, SimplePatternLength)(null),
            "a", Direction.OUTGOING, Seq.empty, "d", "r3", ExpandInto)(null),
          "r1", "a", startInScope = true, "b", endInScope = true, None, directed = true, SimplePatternLength)(null))
    }
  }
}
