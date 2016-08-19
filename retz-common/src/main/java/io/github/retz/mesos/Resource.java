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
package io.github.retz.mesos;

import org.apache.mesos.Protos;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Resource {

    private double cpu;
    private int memMB;
    private int diskMB;
    private int reservedDiskMB;
    private int gpu;
    private final Map<String, Protos.Resource> volumes = new LinkedHashMap<>();

    public Resource(double cpu, int memMB, int diskMB) {
        this(cpu, memMB, diskMB, 0, 0);
    }

    public Resource(double cpu, int memMB, int diskMB, int reservedDiskMB, int gpu) {
        this.cpu = cpu;
        this.memMB = memMB;
        this.diskMB = diskMB;
        this.reservedDiskMB = reservedDiskMB;
        this.gpu = gpu;
    }

    public void merge(Resource rhs) {
        this.cpu += rhs.cpu();
        this.memMB += rhs.memMB();
        this.diskMB += rhs.diskMB();
        this.reservedDiskMB += rhs.reservedDiskMB();
        this.gpu += rhs.gpu();
        this.volumes.putAll(rhs.volumes());
    }

    public double cpu() {
        return cpu;
    }

    public int memMB() {
        return memMB;
    }

    public int diskMB() {
        return diskMB;
    }

    public int reservedDiskMB() {
        return reservedDiskMB;
    }

    public int gpu() {
        return gpu;
    }

    public Map<String, Protos.Resource> volumes() {
        return volumes;
    }

    public String getPersistentVolumePath(String volumeId, String defaultPath) {
        if (!volumes.containsKey(volumeId)) {
            // volume not found
            return defaultPath;
        }
        Protos.Resource r = volumes.get(volumeId);
        return r.getDisk().getVolume().getContainerPath();
    }

    public void subCPU(double cpu) {
        this.cpu -= cpu;
    }

    public void subMemMB(int memMB) {
        this.memMB -= memMB;
    }

    public void subGPU(int gpu) {
        this.gpu -= gpu;
    }

    @Override
    public String toString() {
        List<String> vols = volumes.entrySet().stream()
                .map(entry -> String.format("%s=%fMB:%s:%s",
                        entry.getKey(), entry.getValue().getScalar().getValue(),
                        entry.getValue().getDisk().getVolume().getContainerPath(),
                        entry.getValue().getDisk().getVolume().getHostPath()))
                .collect(Collectors.toList());
        return String.format("cpu=%.1f, mem=%d, disk=%d, reserved_disk=%d, gpus=%d, volumes=[%s]",
                cpu, memMB, diskMB, reservedDiskMB, gpu, String.join(", ", vols));
    }
}
