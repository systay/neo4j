package org.neo4j.cypher.internal.compiler.v3_2.bork.operators;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cypher.internal.compiler.v3_2.bork.Morsel;
import org.neo4j.cypher.internal.compiler.v3_2.bork.Operator;
import org.neo4j.cypher.internal.compiler.v3_2.bork.QueryRun;

public class NodeByLabelScanOp extends Operator {
    private final int offset;
    private final int labelToken;

    public NodeByLabelScanOp(int offset, int labelToken) {
        this.offset = offset;
        this.labelToken = labelToken;
    }

    @Override
    public boolean execute(Morsel input, QueryRun query) {
        PrimitiveLongIterator iter = query.getOrCreate(this,
                () -> query.getStatement().readOperations().nodesGetForLabel(labelToken));

        while(iter.hasNext() && input.createNext()) {
            input.setLongAt(offset, iter.next());
        }

        return iter.hasNext();
    }

    @Override
    public boolean isLeaf() {
        return true;
    }
}
