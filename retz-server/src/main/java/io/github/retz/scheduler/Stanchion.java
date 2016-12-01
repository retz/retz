/**
 *    Retz
 *    Copyright (C) 2016 Nautilus Technologies, Inc.
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

import java.util.Optional;
import java.util.concurrent.*;

// An executor that serializes all request processing here
public class Stanchion {
    private static final Logger LOG = LoggerFactory.getLogger(RetzScheduler.class);

    static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

    static void schedule(Runnable runnable) {
        EXECUTOR.submit( () -> {
            try {
                runnable.run();
            } catch (Exception e) {
                LOG.error("Exception in Stanchion: {}", e.toString(), e);
            }
        });
    }

    public static <R> Optional<R> call(Callable<R> callable) {
        Future<Optional<R>> future = EXECUTOR.submit( () -> {
        try {
            return Optional.of(callable.call());
        } catch (Exception e) {
            LOG.debug(e.toString(), e);
            return Optional.empty();
        }});
        try {
            return future.get();
        } catch (ExecutionException e) {
            LOG.error(e.toString(), e);
        } catch (InterruptedException e) {
            LOG.error(e.toString(), e);
        }
        return Optional.empty();
    }

}
