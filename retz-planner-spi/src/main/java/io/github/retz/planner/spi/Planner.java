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
package io.github.retz.planner.spi;

import io.github.retz.protocol.data.Job;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Retz server will have only one Planner instance within
 * RetzScheduler - so that an actual Planner implemenatation
 * instance may have states inside.
 *
 * Important Note: in 'orderBy', 'filter', 'plan', all of these
 * MUST NOT perform any external I/O like network access, disk access,
 * DB queries as they're in hot path of core scheduling logics
 * of Retz. If any of these methods block, the whole scheduling
 * system may block or freak out. See `Stanchion.java` for how
 * these processes may potentially block.
 *
 * Important Note 2: This Service Provider Interface is still
 * very young boiler plate, which will frequently changed along
 * with future addition of many other built in planners.
 */
public interface Planner {

    // Planners may hold internal states which can be
    // initialized by this method. Exceptions may be
    // thrown if there is any wrong configuration to cancel startup.
    void initialize(Properties p) throws Throwable;

    // Returns list of column names according to sort priority.
    // Strings must exist in 'jobs' table as columns, as these
    // Strings are used for 'ORDER BY' part of SQL query inside
    // Retz.
    //
    // Note: only column names defined in retz.ddl are available,
    // NOT members of Job class.
    // TODO: think of more smarter way of finding these orders
    List<String> orderBy();
    // Selected jobs => true
    // Filtered-out jobs => false
    boolean filter(Job job);

    /**
     * Core callback method that determines matching of jobs and offers,
     * which job assined to which offer (agent) etc etc...
     * Note: attributes in jobs are just a String each but attributes in
     * offers are structured because original attributes in Protos.Offer
     * are already structured (but they're not simple POJO).
     *
     * TODO: offers and jobs should be immutable as they may be reused after plan
     *
     * @param offers A map whose key is offer ID in String with offers
     *               including Resource and Attributes
     * @param jobs   A list of candidate jobs retrieved from DB
     *
     * @return plan for next series of Job dispatch to Mesos
     */
    Plan plan(Map<String, Offer> offers, List<Job> jobs);

    // Official configuration. Frameworks for embedding
    // implementation-specific configurations will be
    // available with other methods.
    void setUseGpu(boolean useGpu);
    void setMaxStock(int maxStock);
}
