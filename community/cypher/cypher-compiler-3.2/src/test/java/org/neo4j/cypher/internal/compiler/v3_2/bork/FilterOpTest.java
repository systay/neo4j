package org.neo4j.cypher.internal.compiler.v3_2.bork;

import org.junit.Test;
import org.neo4j.cypher.internal.compiler.v3_2.bork.operators.FilterOp;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class FilterOpTest {
    @Test
    public void shouldFilterOutStuff() throws Throwable {
        // Given
        RegisterInfo info = new RegisterInfo(1, 0);
        Morsel input = new Morsel(info, 10000);
        Operator lhs = getSourceOperator();

        FilterOp filterOp = new FilterOp(lhs, (input1, queryRun) -> true);

        QueryRun query = new QueryRun(null, null);
        lhs.execute(input, query);
        filterOp.execute(input, query);

        input.resetReadPos();

        int i = 0;
        while(input.next()) {
            i ++;
        }

        assertThat(i, equalTo(10000));
    }

    @Test
    public void shouldFilterOutStuff2() throws Throwable {
        // Given
        RegisterInfo info = new RegisterInfo(1, 0);
        Morsel input = new Morsel(info, 10000);
        Operator lhs = getSourceOperator();

        FilterOp filterOp = new FilterOp(lhs, (input1, queryRun) -> false);

        QueryRun query = new QueryRun(null, null);
        lhs.execute(input, query);
        input.resetReadPos();
        filterOp.execute(input, query);

        input.resetReadPos();

        int i = 0;
        while(input.next()) {
            i ++;
        }


        assertThat(i, equalTo(0));

    }

    private Operator getSourceOperator() {
        return new Operator() {
                @Override
                public boolean execute(Morsel input, QueryRun query) {
                    int i = 0;
                    while (input.createNext()) {
                        input.setLongAt(0, i);
                        i++;
                    }
                    return false;
                }

            @Override
            public boolean isLeaf() {
                return true;
            }
        };
    }


}