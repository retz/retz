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
import io.github.retz.protocol.data.ResourceQuantity;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Retz server will have only one Planner instance within
 * RetzScheduler - so that an actual Planner implemenatation
 * instance may have states inside
 */
public interface Planner {

    // Planners may hold internal states which can be
    // initialized by this method. Exceptions may be
    // thrown if there is any wrong configuration to cancel startup.
    void initialize(Properties p) throws Throwable;

    List<String> orderBy();
    // Selected jobs => true
    // Filtered-out jobs => false
    boolean filter(Job job);

    // TODO: offers and jobs should be immutable as they may be reused after plan
    Plan plan(Map<String, ResourceQuantity> offers, List<Job> jobs);

    // Official configuration. Frameworks for embedding
    // implementation-specific configurations will be
    // available with other methods.
    void setUseGpu(boolean useGpu);
    void setMaxStock(int maxStock);
}
