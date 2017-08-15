/**
 *    Retz
 *    Copyright (C) 2016-2017 Nautilus Technologies, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package io.github.retz.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;

import io.github.retz.misc.LogUtil;

// An executor that serializes all request processing here
public class Stanchion {
    private static final Logger LOG = LoggerFactory.getLogger(RetzScheduler.class);

    static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

    @FunctionalInterface
    public interface RunnableWithException {
        public void run() throws IOException;
    }

    static void schedule(RunnableWithException runnable) {
        EXECUTOR.submit( () -> {
            try {
                runnable.run();
            } catch (Exception e) {
                LogUtil.error(LOG, "Exception in Stanchion", e);
            }
        });
    }

    public static <R> R call(Callable<R> callable) throws IOException {
        Future<R> future = EXECUTOR.submit(callable);
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }
}
