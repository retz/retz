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
package io.github.retz.protocol.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ResourceQuantity {
    private int cpu;
    private int memMB;
    private int gpu;
    private int ports;
    private int diskMB;
    private int nodes;

    public ResourceQuantity() {
        this(0, 0, 0, 0, 0, 0);
    }

    @JsonCreator
    public ResourceQuantity(@JsonProperty("cpu") int cpu,
                            @JsonProperty("memMB") int memMB,
                            @JsonProperty("gpu") int gpu,
                            @JsonProperty("ports") int ports,
                            @JsonProperty("diskMB") int diskMB,
                            @JsonProperty("nodes") int nodes) {
        this.cpu = cpu;
        this.memMB = memMB;
        this.ports = ports;
        this.diskMB = diskMB;
        this.gpu = gpu;
        this.nodes = nodes;
    }

    public void setNodes(int nodes) {
        assert nodes > 0;
        this.nodes = nodes;
    }

    public boolean fits(Job job) {
        return job.cpu() <= cpu &&
                job.memMB() <= memMB &&
                job.gpu() <= gpu &&
                job.ports() <= ports &&
                job.diskMB() <= diskMB;
    }

    @JsonGetter("cpu")
    public int getCpu(){
        return cpu;
    }

    @JsonGetter("memMB")
    public int getMemMB() {
        return memMB;
    }

    @JsonGetter("ports")
    public int getPorts() {
        return ports;
    }

    @JsonGetter("gpu")
    public int getGpu() {
        return gpu;
    }

    @JsonGetter("diskMB")
    public int getDiskMB() {
        return diskMB;
    }

    @JsonGetter("nodes")
    public int getNodes() {
        return nodes;
    }

    public void add(ResourceQuantity resource) {
        this.cpu += resource.getCpu();
        this.memMB += resource.getMemMB();
        this.gpu += resource.getGpu();
        this.ports += resource.getPorts();
        this.diskMB += resource.getDiskMB();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder()
                .append("{cpu=").append(cpu)
                .append(",mem=").append(memMB).append("MB")
                .append(",gpu=").append(gpu)
                .append(",ports=").append(ports)
                .append(",disk=").append(diskMB)
                .append("MB}");
        if (nodes > 0) {
            builder.append("@").append(nodes).append("nodes");
        }
        return builder.toString();
    }
}
