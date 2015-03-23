package org.neo4j.cypher.internal.compiler.v2_3.birk.generated;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cypher.internal.compiler.v2_3.birk.ExecutablePlan;
import org.neo4j.cypher.internal.compiler.v2_3.birk.ResultRow;
import org.neo4j.cypher.internal.compiler.v2_3.birk.ResultRowImpl;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.KernelException;

public class GeneratedExecutionPlan0 implements ExecutablePlan {
    @Override
    public void accept(Visitor<ResultRow, KernelException> visitor, Statement statement, GraphDatabaseService db) throws KernelException {
        ReadOperations ro = statement.readOperations();
        ResultRowImpl row = new ResultRowImpl(db);
        PrimitiveLongIterator v1Iter = ro.nodesGetAll();
        while (v1Iter.hasNext()) {
            long v1 = v1Iter.next();
            PrimitiveLongIterator v3Iter = ro.nodeGetRelationships(v1, Direction.OUTGOING);
            while (v3Iter.hasNext()) {
                long v3 = v3Iter.next();
                long v2 = 666; // Should get the other end of the relationship
                row.setNodeId("a", v1);
                row.setNodeId("b", v2);
                visitor.visit(row);
            }
        }
    }
}
