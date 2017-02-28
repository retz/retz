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

public class PlannerMock implements Planner {

    @Override
    public void initialize(Properties p) {
    }

    @Override
    public boolean filter(Job job) {
        return false;
    }

    @Override
    public List<String> orderBy() {
        return null;
    }

    @Override
    public Plan plan(Map<String, ResourceQuantity> offers, List<Job> jobs) {
        return null;
    }

    @Override
    public void setMaxStock(int maxStock) {
    }

    @Override
    public void setUseGpu(boolean useGpu) {
    }
}
