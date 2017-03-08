/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.executor;

import java.util.concurrent.ExecutorService;

import org.neo4j.unsafe.impl.batchimport.Parallelizable;

/**
 * Like an {@link ExecutorService} with additional absolute control of the current processor count,
 * i.e. the number of threads executing the tasks in parallel.
 *
 * @param <LOCAL> object/state local to each thread, that submitted {@link Task tasks} can get access to
 * when {@link Task#run(Object) running}.
 */
public interface TaskExecutor<LOCAL> extends Parallelizable
{
    int SF_AWAIT_ALL_COMPLETED = 0x1;
    int SF_ABORT_QUEUED = 0x2;

    /**
     * Submits a task to be executed by one of the processors in this {@link TaskExecutor}. Tasks will be
     * executed in the order of which they arrive.
     *
     * @param task a {@link Runnable} defining the task to be executed.
     */
    void submit( Task<LOCAL> task );

    /**
     * Shuts down this {@link TaskExecutor}, disallowing new tasks to be {@link #submit(Task) submitted}.
     *
     * @param flags {@link #SF_AWAIT_ALL_COMPLETED} will wait for all queued or already executing tasks to be
     * executed and completed, before returning from this method. {@link #SF_ABORT_QUEUED} will have
     * submitted tasks which haven't started executing yet cancelled, never to be executed.
     */
    void shutdown( int flags );

    /**
     * @return {@code true} if {@link #shutdown(int)} has been called, otherwise {@code false}.
     */
    boolean isShutdown();

    /**
     * Asserts that this {@link TaskExecutor} is healthy. Useful to call when deciding to wait on a condition
     * this executor is expected to fulfill.
     *
     * @throws RuntimeException of some sort if this executor is in a bad stage, the original error that
     * made this executor fail.
     */
    void assertHealthy();
}