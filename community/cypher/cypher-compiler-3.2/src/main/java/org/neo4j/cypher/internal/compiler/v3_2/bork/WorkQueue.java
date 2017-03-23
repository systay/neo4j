package org.neo4j.cypher.internal.compiler.v3_2.bork;

import org.neo4j.cypher.internal.compiler.v3_2.spi.InternalResultVisitor;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

import java.util.LinkedList;
import java.util.Map;
import java.util.function.Supplier;

import static org.neo4j.kernel.api.security.SecurityContext.AUTH_DISABLED;

public class WorkQueue {

    private final PoolWorker[] threads;
    private final LinkedList<WorkPackage> inQueue;
    private final LinkedList<WorkPackage> outQueue;
    private final Supplier<Statement> statementGetter;
    private final InternalResultVisitor resultVisitor;
    private final Map<Operator, Object> state;
    public int workAdded = 0;
    public int workFinished = 0;

    public WorkQueue(int nThreads, GraphDatabaseQueryService db, Supplier<Statement> statementGetter,
                     InternalResultVisitor resultVisitor, Map<Operator, Object> state) {
        this.statementGetter = statementGetter;
        this.resultVisitor = resultVisitor;
        this.state = state;
        inQueue = new LinkedList<>();
        outQueue = new LinkedList<>();
        threads = new PoolWorker[nThreads];

        for (int i = 0; i < nThreads; i++) {
            threads[i] = new PoolWorker(db);
            threads[i].start();
        }
    }

    public boolean workLeft() {
        return workAdded > workFinished;
    }

    public void execute(WorkPackage r) {
        synchronized (inQueue) {
            inQueue.addLast(r);
            inQueue.notify();
        }
    }

    private void doneExecuting(WorkPackage r) {
        synchronized (outQueue) {
            outQueue.addLast(r);
            outQueue.notify();
        }
    }

    public void shutdown() {
        for (PoolWorker thread : threads) {
            thread.die();
        }
    }

    public WorkPackage waitForFinishedPackage() {
        synchronized (outQueue) {
            while (outQueue.isEmpty()) {
                try {
                    outQueue.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return outQueue.removeFirst();
        }
    }

    private class PoolWorker extends Thread {

        private final GraphDatabaseQueryService db;
        public boolean live = true;

        PoolWorker(GraphDatabaseQueryService db) {
            this.db = db;
        }

        void die() {
            live = false;
        }

        public void run() {
            System.out.println("starting thread");
            try (InternalTransaction tx = db.beginTransaction(KernelTransaction.Type.implicit, AUTH_DISABLED)) {
                Statement statement = statementGetter.get();
                QueryRun queryRun = new QueryRun(statement, resultVisitor, state);

                while (live) {
                    WorkPackage r;

                    synchronized (inQueue) {
                        while (inQueue.isEmpty()) {
                            try {
                                inQueue.wait();
                            } catch (InterruptedException ignored) {
                            }
                        }

                        if (!live) {
                            System.out.println("farewell cruel world!");
                            return;
                        }
                        r = inQueue.removeFirst();
                    }

                    try {
                        r.run(queryRun);
                        doneExecuting(r);
                    } catch (RuntimeException e) {
                        e.printStackTrace();
                    }
                }
                tx.success();
            }
        }
    }


}
