/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.remote.client.http;


import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class RepeatRule implements TestRule {
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.METHOD})
    public @interface Repeat {
        int threads();
        int times();
    }

    private static class RepeatStatement extends Statement {
        private final int times;
        private final int threads;
        private final Statement statement;
        private RepeatStatement(int times, int threads, Statement statement) {
            this.times = times;
            this.threads = threads;
            this.statement = statement;
        }

        @Override
        public void evaluate() throws Throwable {
            final ExecutorService threadPool = Executors.newFixedThreadPool(threads);
            final List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < times; i++) {
                final Future<?> future = threadPool.submit(() -> {
                    try {
                        statement.evaluate();
                    } catch (Throwable throwable) {
                        throw new RuntimeException(throwable);
                    }
                });
                futures.add(future);
            }
            threadPool.shutdown();

            futures.forEach(future -> {
                try {
                    future.get(5, TimeUnit.MINUTES);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        Statement result = statement;
        Repeat repeat = description.getAnnotation(Repeat.class);
        if (repeat != null) {
            result = new RepeatStatement(repeat.times(), repeat.threads(), statement);
        }
        return result;
    }
}

