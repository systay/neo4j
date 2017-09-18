package org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.expressions.compiled;

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext;
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryState;
import org.neo4j.values.AnyValue;

public class TestarLost implements CompiledExpression {
    public AnyValue execute(ExecutionContext row, QueryState state) {
        return org.neo4j.values.storable.Values.NO_VALUE;
    }

    public TestarLost() {
        super();
    }
}
