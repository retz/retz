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
package io.github.retz.web;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.db.Database;
import io.github.retz.protocol.StatusResponse;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.ResourceQuantity;
import io.github.retz.scheduler.RetzScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by kuenishi on 16/12/19.
 */
public class StatusCache implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(StatusCache.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static boolean on = true;
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final StatusResponse statusResponseCache = new StatusResponse(RetzScheduler.HTTP_SERVER_NAME);

    private final int INTERVAL;

    static {
        MAPPER.registerModule(new Jdk8Module());
    }

    StatusCache(int interval) {
        this.INTERVAL = interval;
    }

    @Override
    public void run() {
        StatusCache.updateUsedResources();
        if (on) {
            scheduler.schedule(new StatusCache(INTERVAL), INTERVAL, TimeUnit.SECONDS);
        }
    }

    static void start(int interval) {
        LOG.info("Starting status cache updater with interval={}s", interval);
        scheduler.schedule(new StatusCache(interval), interval, TimeUnit.SECONDS);
    }

    static void stop() {
        on = false;
    }

    static String getStatusResponse() throws JsonProcessingException {
        synchronized (statusResponseCache) {
            return MAPPER.writeValueAsString(statusResponseCache);
        }
    }


    public static void setOfferStats(int size, ResourceQuantity offered) {
        synchronized (statusResponseCache) {
            statusResponseCache.setOffers(size, offered);
        }
    }

    public static void updateUsedResources() {
        // Do cache update here
        LOG.info("try!");
        int queueLength = Database.getInstance().countQueued();
        List<Job> jobs = Database.getInstance().getRunning();
        ResourceQuantity total = new ResourceQuantity();
        for (Job  job : jobs ){
            total.add(new ResourceQuantity(job.cpu(), job.memMB(), job.gpu(), job.ports(), job.diskMB(),  0));
        }
        int running = jobs.size();
        StatusCache.setUsedResources(queueLength, running, total);

        LOG.debug("poke len(Q)={}, len(Running)={}, totalUsed={}",
                queueLength, running, total);
    }

    public static void setUsedResources(int queueLength, int runningLenght, ResourceQuantity totalUsed) {
        synchronized (statusResponseCache) {
            statusResponseCache.setUsedResources(queueLength, runningLenght, totalUsed);
        }
    }
}
