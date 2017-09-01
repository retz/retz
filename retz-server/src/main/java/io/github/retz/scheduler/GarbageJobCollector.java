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

import io.github.retz.db.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class GarbageJobCollector implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(GarbageJobCollector.class);
    private static boolean on = true;
    private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1);

    private final int leeway;
    private final int interval;
    GarbageJobCollector(int leeway, int interval) {
        this.leeway = leeway;
        this.interval = interval;
    }
    @Override
    public void run() {
        LOG.debug("beep! on={}, leeway={}", true, leeway);
        try {
            Database.getInstance().deleteOldJobs(leeway);
        } catch (Throwable t) {
            LOG.warn(t.toString(), t);
        }
        if (on) {
            SCHEDULER.schedule(new GarbageJobCollector(leeway, interval), interval, TimeUnit.SECONDS);
        }
    }
    static void start(int leeway, int interval) {
        LOG.info("Starting garbage job collector with leeway={}s, interval={}s", leeway, interval);
        SCHEDULER.schedule(new GarbageJobCollector(leeway, interval), interval, TimeUnit.SECONDS);
    }

    static void stop() {
        on = false;
    }
}
