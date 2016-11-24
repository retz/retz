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

public class ResourceQuantity {
    private int cpu;
    private int memMB;
    private int gpu;
    private int ports;
    private int diskMB;

    public ResourceQuantity(int cpu, int memMB, int gpu, int ports, int diskMB) {
        this.cpu = cpu;
        this.memMB = memMB;
        this.ports = ports;
        this.diskMB = diskMB;
        this.gpu = gpu;
    }

    public boolean fits(Job job) {
        return job.cpu() <= cpu &&
                job.memMB() <= memMB &&
                job.gpu() <= gpu &&
                job.ports() <= ports &&
                job.diskMB() <= diskMB;
    }

    public int getCpu(){
        return cpu;
    }

    public int getMemMB() {
        return memMB;
    }

    public int getPorts() {
        return ports;
    }

    public int getGpu() {
        return gpu;
    }

    public int getDiskMB() {
        return diskMB;
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
        return builder.toString();
    }
}
