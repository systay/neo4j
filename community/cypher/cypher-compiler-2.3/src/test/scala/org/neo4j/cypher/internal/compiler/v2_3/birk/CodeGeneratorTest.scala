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

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.ast.{LabelName, LabelToken}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_3.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.graphdb.Direction

class CodeGeneratorTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val generator = new CodeGenerator

  test("all nodes scan") { // MATCH a RETURN a
    val plan = ProduceResult(List("a"), AllNodesScan(IdName("a"), Set.empty)(solved))

    generator.generate(plan, "AllNodeScan") should equal("""package org.neo4j.cypher.internal.compiler.v2_3.birk.generated;
                        |import org.neo4j.helpers.collection.Visitor;
                        |import org.neo4j.graphdb.GraphDatabaseService;
                        |import org.neo4j.kernel.api.Statement;
                        |import org.neo4j.kernel.api.exceptions.KernelException;
                        |import org.neo4j.kernel.api.ReadOperations;
                        |import org.neo4j.cypher.internal.compiler.v2_3.birk.ExecutablePlan;
                        |import org.neo4j.cypher.internal.compiler.v2_3.birk.ResultRow;
                        |import org.neo4j.cypher.internal.compiler.v2_3.birk.ResultRowImpl;
                        |import org.neo4j.collection.primitive.PrimitiveLongIterator;
                        |public class AllNodeScan implements ExecutablePlan
                        |{
                        |  @Override
                        |  public void accept(final Visitor<ResultRow, KernelException> visitor, final Statement statement, GraphDatabaseService db) throws KernelException
                        |  {
                        |    final ReadOperations ro = statement.readOperations();
                        |    final ResultRowImpl row = new ResultRowImpl(db);
                        |    PrimitiveLongIterator v1Iter = ro.nodesGetAll();
                        |    while ( v1Iter.hasNext() )
                        |    {
                        |      final long v1 = v1Iter.next();
                        |      row.setNodeId("a", v1);
                        |      visitor.visit(row);
                        |    }
                        |  }
                        |}""".stripMargin)
  }

  test("label scan") {
    // MATCH (a:L) RETURN a
    val plan = ProduceResult(List("a"), NodeByLabelScan(IdName("a"), LazyLabel("L"), Set.empty)(solved))

    generator.generate(plan, "NodeByLabelScan") should equal("""package org.neo4j.cypher.internal.compiler.v2_3.birk.generated;
                                            |import org.neo4j.helpers.collection.Visitor;
                                            |import org.neo4j.graphdb.GraphDatabaseService;
                                            |import org.neo4j.kernel.api.Statement;
                                            |import org.neo4j.kernel.api.exceptions.KernelException;
                                            |import org.neo4j.kernel.api.ReadOperations;
                                            |import org.neo4j.cypher.internal.compiler.v2_3.birk.ExecutablePlan;
                                            |import org.neo4j.cypher.internal.compiler.v2_3.birk.ResultRow;
                                            |import org.neo4j.cypher.internal.compiler.v2_3.birk.ResultRowImpl;
                                            |import org.neo4j.collection.primitive.PrimitiveLongIterator;
                                            |public class NodeByLabelScan implements ExecutablePlan
                                            |{
                                            |  @Override
                                            |  public void accept(final Visitor<ResultRow, KernelException> visitor, final Statement statement, GraphDatabaseService db) throws KernelException
                                            |  {
                                            |    final ReadOperations ro = statement.readOperations();
                                            |    final ResultRowImpl row = new ResultRowImpl(db);
                                            |    int v2 = ro.labelGetForName( "L" );
                                            |    PrimitiveLongIterator v1Iter = ro.nodesGetForLabel( v2 );
                                            |    while ( v1Iter.hasNext() )
                                            |    {
                                            |      final long v1 = v1Iter.next();
                                            |      row.setNodeId("a", v1);
                                            |      visitor.visit(row);
                                            |    }
                                            |  }
                                            |}""".stripMargin)
  }


  test("hash join of all nodes scans") { // MATCH a RETURN a
    val lhs = AllNodesScan(IdName("a"), Set.empty)(solved)
    val rhs = AllNodesScan(IdName("a"), Set.empty)(solved)
    val join = NodeHashJoin(Set(IdName("a")), lhs, rhs)(solved)
    val plan = ProduceResult(List("a"), join)
    generator.generate(plan, "HashJoin") should equal ("""package org.neo4j.cypher.internal.compiler.v2_3.birk.generated;
                             |import org.neo4j.helpers.collection.Visitor;
                             |import org.neo4j.graphdb.GraphDatabaseService;
                             |import org.neo4j.kernel.api.Statement;
                             |import org.neo4j.kernel.api.exceptions.KernelException;
                             |import org.neo4j.kernel.api.ReadOperations;
                             |import org.neo4j.cypher.internal.compiler.v2_3.birk.ExecutablePlan;
                             |import org.neo4j.cypher.internal.compiler.v2_3.birk.ResultRow;
                             |import org.neo4j.cypher.internal.compiler.v2_3.birk.ResultRowImpl;
                             |import org.neo4j.collection.primitive.Primitive;
                             |import org.neo4j.collection.primitive.PrimitiveLongIntMap;
                             |import org.neo4j.collection.primitive.PrimitiveLongIterator;
                             |import org.neo4j.collection.primitive.hopscotch.LongKeyIntValueTable;
                             |public class HashJoin implements ExecutablePlan
                             |{
                             |  @Override
                             |  public void accept(final Visitor<ResultRow, KernelException> visitor, final Statement statement, GraphDatabaseService db) throws KernelException
                             |  {
                             |    final ReadOperations ro = statement.readOperations();
                             |    final ResultRowImpl row = new ResultRowImpl(db);
                             |    final PrimitiveLongIntMap v2 = m1(ro);
                             |    PrimitiveLongIterator v4Iter = ro.nodesGetAll();
                             |    while ( v4Iter.hasNext() )
                             |    {
                             |      final long v4 = v4Iter.next();
                             |      int v3 = v2.get( v4);
                             |      if ( v3 != LongKeyIntValueTable.NULL )
                             |      {
                             |        for ( int i = 0; i < v3; i++ )
                             |        {
                             |          row.setNodeId("a", v4);
                             |          visitor.visit(row);
                             |        }
                             |      }
                             |    }
                             |  }
                             |  private PrimitiveLongIntMap m1(ReadOperations ro) throws KernelException
                             |  {
                             |    final PrimitiveLongIntMap v2 = Primitive.longIntMap();
                             |    PrimitiveLongIterator v1Iter = ro.nodesGetAll();
                             |    while ( v1Iter.hasNext() )
                             |    {
                             |      final long v1 = v1Iter.next();
                             |      int count = v2.get( v1 );
                             |      if ( count == LongKeyIntValueTable.NULL )
                             |      {
                             |        v2.put( v1, 1 );
                             |      }
                             |      else
                             |      {
                             |        v2.put( v1, count + 1 );
                             |      }
                             |    }
                             |    return v2;
                             |  }
                             |}""".stripMargin)
  }

  test("all nodes scan + expand") { // MATCH (a)-[r]->(b) RETURN a, b
    val plan = ProduceResult(List("a", "b"),
      Expand(
        AllNodesScan(IdName("a"), Set.empty)(solved), IdName("a"), Direction.OUTGOING, Seq.empty, IdName("b"), IdName("r"), ExpandAll)(solved))


    generator.generate(plan, "AllNodeScanAndExpand") should equal("""package org.neo4j.cypher.internal.compiler.v2_3.birk.generated;
                        |import org.neo4j.helpers.collection.Visitor;
                        |import org.neo4j.graphdb.GraphDatabaseService;
                        |import org.neo4j.kernel.api.Statement;
                        |import org.neo4j.kernel.api.exceptions.KernelException;
                        |import org.neo4j.kernel.api.ReadOperations;
                        |import org.neo4j.cypher.internal.compiler.v2_3.birk.ExecutablePlan;
                        |import org.neo4j.cypher.internal.compiler.v2_3.birk.ResultRow;
                        |import org.neo4j.cypher.internal.compiler.v2_3.birk.ResultRowImpl;
                        |import org.neo4j.collection.primitive.Primitive;
                        |import org.neo4j.collection.primitive.PrimitiveLongIterator;
                        |import org.neo4j.graphdb.Direction;
                        |import org.neo4j.kernel.api.exceptions.KernelException;
                        |import org.neo4j.kernel.impl.api.RelationshipVisitor;
                        |public class AllNodeScanAndExpand implements ExecutablePlan
                        |{
                        |  @Override
                        |  public void accept(final Visitor<ResultRow, KernelException> visitor, final Statement statement, GraphDatabaseService db) throws KernelException
                        |  {
                        |    final ReadOperations ro = statement.readOperations();
                        |    final ResultRowImpl row = new ResultRowImpl(db);
                        |    PrimitiveLongIterator v1Iter = ro.nodesGetAll();
                        |    while ( v1Iter.hasNext() )
                        |    {
                        |      final long v1 = v1Iter.next();
                        |      PrimitiveLongIterator v2Iter = ro.nodeGetRelationships(v1, Direction.OUTGOING);
                        |      while ( v2Iter.hasNext() )
                        |      {
                        |        final long v2 = v2Iter.next();
                        |        ro.relationshipVisit( v2, new RelationshipVisitor<KernelException>()
                        |        {
                        |          @Override
                        |          public void visit( long relId, int type, long startNode, long v3 ) throws KernelException
                        |          {
                        |            row.setNodeId("a", v1);
                        |            row.setNodeId("b", v3);
                        |            visitor.visit(row);
                        |          }
                        |          });
                        |        }
                        |      }
                        |    }
                        |  }""".stripMargin)
  }

  test("hash join on top of two expands from two all node scans") {
    // MATCH (a)-[r1]->(b)<-[r2]-(c) RETURN b
    val generator = new CodeGenerator
    val lhs = Expand(AllNodesScan(IdName("a"), Set.empty)(solved), IdName("a"), Direction.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), ExpandAll)(solved)
    val rhs = Expand(AllNodesScan(IdName("c"), Set.empty)(solved), IdName("c"), Direction.INCOMING, Seq.empty, IdName("b"), IdName("r2"), ExpandAll)(solved)
    val join = NodeHashJoin(Set(IdName("b")), lhs, rhs)(solved)
    val plan = ProduceResult(List("b"), join)

    generator.generate(plan, "HashJoinExpand") should equal("""package org.neo4j.cypher.internal.compiler.v2_3.birk.generated;
                            |import org.neo4j.helpers.collection.Visitor;
                            |import org.neo4j.graphdb.GraphDatabaseService;
                            |import org.neo4j.kernel.api.Statement;
                            |import org.neo4j.kernel.api.exceptions.KernelException;
                            |import org.neo4j.kernel.api.ReadOperations;
                            |import org.neo4j.cypher.internal.compiler.v2_3.birk.ExecutablePlan;
                            |import org.neo4j.cypher.internal.compiler.v2_3.birk.ResultRow;
                            |import org.neo4j.cypher.internal.compiler.v2_3.birk.ResultRowImpl;
                            |import java.util.ArrayList;
                            |import org.neo4j.collection.primitive.Primitive;
                            |import org.neo4j.collection.primitive.PrimitiveLongIterator;
                            |import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
                            |import org.neo4j.graphdb.Direction;
                            |import org.neo4j.kernel.api.exceptions.KernelException;
                            |import org.neo4j.kernel.impl.api.RelationshipVisitor;
                            |public class HashJoinExpand implements ExecutablePlan
                            |{
                            |  static class ValueTypeInv4
                            |  {
                            |    long a;
                            |    long r1;
                            |  }
                            |  @Override
                            |  public void accept(final Visitor<ResultRow, KernelException> visitor, final Statement statement, GraphDatabaseService db) throws KernelException
                            |  {
                            |    final ReadOperations ro = statement.readOperations();
                            |    final ResultRowImpl row = new ResultRowImpl(db);
                            |    final PrimitiveLongObjectMap<ArrayList<ValueTypeInv4>> v4 = m1(ro);
                            |    PrimitiveLongIterator v9Iter = ro.nodesGetAll();
                            |    while ( v9Iter.hasNext() )
                            |    {
                            |      final long v9 = v9Iter.next();
                            |      PrimitiveLongIterator v10Iter = ro.nodeGetRelationships(v9, Direction.INCOMING);
                            |      while ( v10Iter.hasNext() )
                            |      {
                            |        final long v10 = v10Iter.next();
                            |        ro.relationshipVisit( v10, new RelationshipVisitor<KernelException>()
                            |        {
                            |          @Override
                            |          public void visit( long relId, int type, long startNode, long v11 ) throws KernelException
                            |          {
                            |            ArrayList<ValueTypeInv4> v7 = v4.get( v11);
                            |            if ( v7!= null )
                            |            {
                            |              for (ValueTypeInv4 v8 : v7 )
                            |              {
                            |                final long v5 = v8.a;
                            |                final long v6 = v8.r1;
                            |                row.setNodeId("b", v11);
                            |                visitor.visit(row);
                            |              }
                            |            }
                            |          }
                            |          });
                            |        }
                            |      }
                            |    }
                            |    private PrimitiveLongObjectMap<ArrayList<ValueTypeInv4>> m1(ReadOperations ro) throws KernelException
                            |    {
                            |      final PrimitiveLongObjectMap<ArrayList<ValueTypeInv4>> v4 = Primitive.longObjectMap( );
                            |      PrimitiveLongIterator v1Iter = ro.nodesGetAll();
                            |      while ( v1Iter.hasNext() )
                            |      {
                            |        final long v1 = v1Iter.next();
                            |        PrimitiveLongIterator v2Iter = ro.nodeGetRelationships(v1, Direction.OUTGOING);
                            |        while ( v2Iter.hasNext() )
                            |        {
                            |          final long v2 = v2Iter.next();
                            |          ro.relationshipVisit( v2, new RelationshipVisitor<KernelException>()
                            |          {
                            |            @Override
                            |            public void visit( long relId, int type, long startNode, long v3 ) throws KernelException
                            |            {
                            |              ArrayList<ValueTypeInv4> v12 = v4.get( v3 );
                            |              if ( null == v12 )
                            |              {
                            |                v4.put( v3, (v12 = new ArrayList<>( ) ) );
                            |              }
                            |              ValueTypeInv4 v13  = new ValueTypeInv4();
                            |              v13.a = v1;
                            |              v13.r1 = v2;
                            |              v12.add( v13 );
                            |            }
                            |            });
                            |          }
                            |        }
                            |        return v4;
                            |      }
                            |    }""".stripMargin)
  }

  test("hash join on top of two expands from two label scans") {
    // MATCH (a:T1)-[r1]->(b)<-[r2]-(c:T2) RETURN b
    val generator = new CodeGenerator
    val lhs = Expand(NodeByLabelScan(IdName("a"), LazyLabel("T1"), Set.empty)(solved), IdName("a"), Direction.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), ExpandAll)(solved)
    val rhs = Expand(NodeByLabelScan(IdName("c"), LazyLabel("T2"), Set.empty)(solved), IdName("c"), Direction.INCOMING, Seq.empty, IdName("b"), IdName("r2"), ExpandAll)(solved)
    val join = NodeHashJoin(Set(IdName("b")), lhs, rhs)(solved)
    val plan = ProduceResult(List("b"), join)

    generator.generate(plan, "HashJoinExpand") should equal("""package org.neo4j.cypher.internal.compiler.v2_3.birk.generated;
                            |import org.neo4j.helpers.collection.Visitor;
                            |import org.neo4j.graphdb.GraphDatabaseService;
                            |import org.neo4j.kernel.api.Statement;
                            |import org.neo4j.kernel.api.exceptions.KernelException;
                            |import org.neo4j.kernel.api.ReadOperations;
                            |import org.neo4j.cypher.internal.compiler.v2_3.birk.ExecutablePlan;
                            |import org.neo4j.cypher.internal.compiler.v2_3.birk.ResultRow;
                            |import org.neo4j.cypher.internal.compiler.v2_3.birk.ResultRowImpl;
                            |import java.util.ArrayList;
                            |import org.neo4j.collection.primitive.Primitive;
                            |import org.neo4j.collection.primitive.PrimitiveLongIterator;
                            |import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
                            |import org.neo4j.graphdb.Direction;
                            |import org.neo4j.kernel.api.exceptions.KernelException;
                            |import org.neo4j.kernel.impl.api.RelationshipVisitor;
                            |public class HashJoinExpand implements ExecutablePlan
                            |{
                            |  static class ValueTypeInv5
                            |  {
                            |    long a;
                            |    long r1;
                            |  }
                            |  @Override
                            |  public void accept(final Visitor<ResultRow, KernelException> visitor, final Statement statement, GraphDatabaseService db) throws KernelException
                            |  {
                            |    final ReadOperations ro = statement.readOperations();
                            |    final ResultRowImpl row = new ResultRowImpl(db);
                            |    int v11 = ro.labelGetForName( "T2" );
                            |    final PrimitiveLongObjectMap<ArrayList<ValueTypeInv5>> v5 = m1(ro);
                            |    PrimitiveLongIterator v10Iter = ro.nodesGetForLabel( v11 );
                            |    while ( v10Iter.hasNext() )
                            |    {
                            |      final long v10 = v10Iter.next();
                            |      PrimitiveLongIterator v12Iter = ro.nodeGetRelationships(v10, Direction.INCOMING);
                            |      while ( v12Iter.hasNext() )
                            |      {
                            |        final long v12 = v12Iter.next();
                            |        ro.relationshipVisit( v12, new RelationshipVisitor<KernelException>()
                            |        {
                            |          @Override
                            |          public void visit( long relId, int type, long startNode, long v13 ) throws KernelException
                            |          {
                            |            ArrayList<ValueTypeInv5> v8 = v5.get( v13);
                            |            if ( v8!= null )
                            |            {
                            |              for (ValueTypeInv5 v9 : v8 )
                            |              {
                            |                final long v6 = v9.a;
                            |                final long v7 = v9.r1;
                            |                row.setNodeId("b", v13);
                            |                visitor.visit(row);
                            |              }
                            |            }
                            |          }
                            |          });
                            |        }
                            |      }
                            |    }
                            |    private PrimitiveLongObjectMap<ArrayList<ValueTypeInv5>> m1(ReadOperations ro) throws KernelException
                            |    {
                            |      int v2 = ro.labelGetForName( "T1" );
                            |      final PrimitiveLongObjectMap<ArrayList<ValueTypeInv5>> v5 = Primitive.longObjectMap( );
                            |      PrimitiveLongIterator v1Iter = ro.nodesGetForLabel( v2 );
                            |      while ( v1Iter.hasNext() )
                            |      {
                            |        final long v1 = v1Iter.next();
                            |        PrimitiveLongIterator v3Iter = ro.nodeGetRelationships(v1, Direction.OUTGOING);
                            |        while ( v3Iter.hasNext() )
                            |        {
                            |          final long v3 = v3Iter.next();
                            |          ro.relationshipVisit( v3, new RelationshipVisitor<KernelException>()
                            |          {
                            |            @Override
                            |            public void visit( long relId, int type, long startNode, long v4 ) throws KernelException
                            |            {
                            |              ArrayList<ValueTypeInv5> v14 = v5.get( v4 );
                            |              if ( null == v14 )
                            |              {
                            |                v5.put( v4, (v14 = new ArrayList<>( ) ) );
                            |              }
                            |              ValueTypeInv5 v15  = new ValueTypeInv5();
                            |              v15.a = v1;
                            |              v15.r1 = v3;
                            |              v14.add( v15 );
                            |            }
                            |            });
                            |          }
                            |        }
                            |        return v5;
                            |      }
                            |    }""".stripMargin)
  }

  test("hash join double") {
    // MATCH (a)-[r1]->(b)<-[r2]-(c)<-[r3]-(d) RETURN b
    val generator = new CodeGenerator

    val lhs = Expand(AllNodesScan(IdName("a"), Set.empty)(solved), IdName("a"), Direction.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), ExpandAll)(solved)
    val rhs1 = Expand(AllNodesScan(IdName("c"), Set.empty)(solved), IdName("c"), Direction.INCOMING, Seq.empty, IdName("b"), IdName("r2"), ExpandAll)(solved)
    val rhs2 = Expand(AllNodesScan(IdName("d"), Set.empty)(solved), IdName("d"), Direction.INCOMING, Seq.empty, IdName("c"), IdName("r3"), ExpandAll)(solved)
    val join1 = NodeHashJoin(Set(IdName("b")), lhs, rhs1)(solved)
    val join2 = NodeHashJoin(Set(IdName("c")), join1, rhs2)(solved)
    val plan = ProduceResult(List("b"), join2)

    generator.generate(plan) should equal("""package org.neo4j.cypher.internal.compiler.v2_3.birk.generated;
                                            |import org.neo4j.helpers.collection.Visitor;
                                            |import org.neo4j.graphdb.GraphDatabaseService;
                                            |import org.neo4j.kernel.api.Statement;
                                            |import org.neo4j.kernel.api.exceptions.KernelException;
                                            |import org.neo4j.kernel.api.ReadOperations;
                                            |import org.neo4j.cypher.internal.compiler.v2_3.birk.ExecutablePlan;
                                            |import org.neo4j.cypher.internal.compiler.v2_3.birk.ResultRow;
                                            |import org.neo4j.cypher.internal.compiler.v2_3.birk.ResultRowImpl;
                                            |import java.util.ArrayList;
                                            |import org.neo4j.collection.primitive.Primitive;
                                            |import org.neo4j.collection.primitive.PrimitiveLongIterator;
                                            |import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
                                            |import org.neo4j.graphdb.Direction;
                                            |import org.neo4j.kernel.api.exceptions.KernelException;
                                            |import org.neo4j.kernel.impl.api.RelationshipVisitor;
                                            |public class GeneratedExecutionPlan0 implements ExecutablePlan
                                            |{
                                            |  static class ValueTypeInv4
                                            |  {
                                            |    long a;
                                            |    long r1;
                                            |  }
                                            |  static class ValueTypeInv12
                                            |  {
                                            |    long a;
                                            |    long b;
                                            |    long r1;
                                            |    long r2;
                                            |  }
                                            |  @Override
                                            |  public void accept(final Visitor<ResultRow, KernelException> visitor, final Statement statement, GraphDatabaseService db) throws KernelException
                                            |  {
                                            |    final ReadOperations ro = statement.readOperations();
                                            |    final ResultRowImpl row = new ResultRowImpl(db);
                                            |    final PrimitiveLongObjectMap<ArrayList<ValueTypeInv12>> v12 = m2(ro);
                                            |    PrimitiveLongIterator v19Iter = ro.nodesGetAll();
                                            |    while ( v19Iter.hasNext() )
                                            |    {
                                            |      final long v19 = v19Iter.next();
                                            |      PrimitiveLongIterator v20Iter = ro.nodeGetRelationships(v19, Direction.INCOMING);
                                            |      while ( v20Iter.hasNext() )
                                            |      {
                                            |        final long v20 = v20Iter.next();
                                            |        ro.relationshipVisit( v20, new RelationshipVisitor<KernelException>()
                                            |        {
                                            |          @Override
                                            |          public void visit( long relId, int type, long startNode, long v21 ) throws KernelException
                                            |          {
                                            |            ArrayList<ValueTypeInv12> v17 = v12.get( v21);
                                            |            if ( v17!= null )
                                            |            {
                                            |              for (ValueTypeInv12 v18 : v17 )
                                            |              {
                                            |                final long v13 = v18.a;
                                            |                final long v14 = v18.b;
                                            |                final long v15 = v18.r1;
                                            |                final long v16 = v18.r2;
                                            |                row.setNodeId("b", v14);
                                            |                visitor.visit(row);
                                            |              }
                                            |            }
                                            |          }
                                            |          });
                                            |        }
                                            |      }
                                            |    }
                                            |    private PrimitiveLongObjectMap<ArrayList<ValueTypeInv4>> m1(ReadOperations ro) throws KernelException
                                            |    {
                                            |      final PrimitiveLongObjectMap<ArrayList<ValueTypeInv4>> v4 = Primitive.longObjectMap( );
                                            |      PrimitiveLongIterator v1Iter = ro.nodesGetAll();
                                            |      while ( v1Iter.hasNext() )
                                            |      {
                                            |        final long v1 = v1Iter.next();
                                            |        PrimitiveLongIterator v2Iter = ro.nodeGetRelationships(v1, Direction.OUTGOING);
                                            |        while ( v2Iter.hasNext() )
                                            |        {
                                            |          final long v2 = v2Iter.next();
                                            |          ro.relationshipVisit( v2, new RelationshipVisitor<KernelException>()
                                            |          {
                                            |            @Override
                                            |            public void visit( long relId, int type, long startNode, long v3 ) throws KernelException
                                            |            {
                                            |              ArrayList<ValueTypeInv4> v22 = v4.get( v3 );
                                            |              if ( null == v22 )
                                            |              {
                                            |                v4.put( v3, (v22 = new ArrayList<>( ) ) );
                                            |              }
                                            |              ValueTypeInv4 v23  = new ValueTypeInv4();
                                            |              v23.a = v1;
                                            |              v23.r1 = v2;
                                            |              v22.add( v23 );
                                            |            }
                                            |            });
                                            |          }
                                            |        }
                                            |        return v4;
                                            |      }
                                            |      private PrimitiveLongObjectMap<ArrayList<ValueTypeInv12>> m2(ReadOperations ro) throws KernelException
                                            |      {
                                            |        final PrimitiveLongObjectMap<ArrayList<ValueTypeInv12>> v12 = Primitive.longObjectMap( );
                                            |        final PrimitiveLongObjectMap<ArrayList<ValueTypeInv4>> v4 = m1(ro);
                                            |        PrimitiveLongIterator v9Iter = ro.nodesGetAll();
                                            |        while ( v9Iter.hasNext() )
                                            |        {
                                            |          final long v9 = v9Iter.next();
                                            |          PrimitiveLongIterator v10Iter = ro.nodeGetRelationships(v9, Direction.INCOMING);
                                            |          while ( v10Iter.hasNext() )
                                            |          {
                                            |            final long v10 = v10Iter.next();
                                            |            ro.relationshipVisit( v10, new RelationshipVisitor<KernelException>()
                                            |            {
                                            |              @Override
                                            |              public void visit( long relId, int type, long startNode, long v11 ) throws KernelException
                                            |              {
                                            |                ArrayList<ValueTypeInv4> v7 = v4.get( v11);
                                            |                if ( v7!= null )
                                            |                {
                                            |                  for (ValueTypeInv4 v8 : v7 )
                                            |                  {
                                            |                    final long v5 = v8.a;
                                            |                    final long v6 = v8.r1;
                                            |                    ArrayList<ValueTypeInv12> v24 = v12.get( v9 );
                                            |                    if ( null == v24 )
                                            |                    {
                                            |                      v12.put( v9, (v24 = new ArrayList<>( ) ) );
                                            |                    }
                                            |                    ValueTypeInv12 v25  = new ValueTypeInv12();
                                            |                    v25.a = v5;
                                            |                    v25.b = v11;
                                            |                    v25.r1 = v6;
                                            |                    v25.r2 = v10;
                                            |                    v24.add( v25 );
                                            |                  }
                                            |                }
                                            |              }
                                            |              });
                                            |            }
                                            |          }
                                            |          return v12;
                                            |        }
                                            |      }""".stripMargin)
  }
}
