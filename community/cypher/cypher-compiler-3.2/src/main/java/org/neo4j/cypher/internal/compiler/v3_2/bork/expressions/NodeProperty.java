package org.neo4j.cypher.internal.compiler.v3_2.bork.expressions;

import org.neo4j.cypher.internal.compiler.v3_2.bork.Expression;
import org.neo4j.cypher.internal.compiler.v3_2.bork.QueryRun;
import org.neo4j.cypher.internal.compiler.v3_2.bork.Register;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public class NodeProperty implements Expression {

    private final int nodeOffset;
    private final int propKeyToken;

    public NodeProperty(int nodeOffset, int propKeyToken) {
        this.nodeOffset = nodeOffset;
        this.propKeyToken = propKeyToken;
    }

    @Override
    public Object evaluate(Register input, QueryRun queryRun) throws EntityNotFoundException {

        long nodeId = input.getLongAt(nodeOffset);
        return queryRun.getStatement().readOperations().nodeGetProperty(nodeId, propKeyToken);
    }
}
