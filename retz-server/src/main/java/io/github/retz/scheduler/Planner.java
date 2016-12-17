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

import io.github.retz.protocol.data.Job;
import org.apache.mesos.Protos;

import java.util.List;

public interface Planner {

    List<String> orderBy();

    // TODO: make useGPU and maxStock configuration of each instance
    List<AppJobPair> filter(List<Job> jobs, List<Job> cancel, boolean useGPU);

    Plan plan(List<Protos.Offer> offers, List<AppJobPair> jobs, int maxStock, String unixUser);
}