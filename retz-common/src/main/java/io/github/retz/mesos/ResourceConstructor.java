/**
 *    Retz
 *    Copyright (C) 2016 Nautilus Technologies, KK.
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ResourceConstructor {
    public static List<Protos.Resource> construct(int cpus, int memMB) {
        List<Protos.Resource> list = new LinkedList<Protos.Resource>();
        list.add(Protos.Resource.newBuilder()
                .setName("cpus")
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpus).build())
                .setType(Protos.Value.Type.SCALAR)
                .build());
        list.add(Protos.Resource.newBuilder()
                .setName("mem")
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(memMB))
                .setType(Protos.Value.Type.SCALAR)
                .build());
        return list;
    }

    public static List<Protos.Resource> construct(int cpus, int memMB, int disk, int gpu) {
        List<Protos.Resource> list = construct(cpus, memMB);
        list.add(buildDisk(disk));
        list.add(Protos.Resource.newBuilder()
                .setName("gpus")
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(gpu))
                .setType(Protos.Value.Type.SCALAR)
                .build());
        return list;
    }

    public static Protos.Resource buildDisk(int sizeMB) {
        return Protos.Resource.newBuilder()
                .setName("disk")
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(sizeMB))
                .setType(Protos.Value.Type.SCALAR)
                .build();
    }

    public static Resource decode(List<Protos.Resource> resources) {
        int memMB = 0, diskMB = 0, reservedDiskMB = 0, gpu = 0;
        double cpu = 0;
        Map<String, Protos.Resource> volumes = new LinkedHashMap<>();
        for (Protos.Resource r : resources) {
            if (r.getName().equals("cpus")) {
                // Will be screwed up if we have reserved CPUs
                cpu = r.getScalar().getValue();
            } else if (r.getName().equals("mem")) {
                // Will be screwed up if we have reserved CPUs
                memMB = (int) (r.getScalar().getValue());
            } else if (r.getName().equals("disk")) {
                int size = (int) (r.getScalar().getValue());

                if (r.hasReservation()) {
                    if (r.hasDisk() && r.getDisk().hasVolume()) {
                        volumes.put(r.getDisk().getPersistence().getId(), r);
                    } else {
                        // TODO: check principal and owner is right ones
                        reservedDiskMB += size;
                    }
                } else {
                    diskMB = size;
                }
                /*
                System.err.println("================================");
                System.err.println(diskMB);
                System.err.println(reservedDiskMB);
                System.err.println("Reservation Principal> " + r.getReservation().getPrincipal());
                System.err.println("Reservation Role> " + r.getRole()); //.getDisk().getPersistence().getId());
                System.err.println("Volume ID> " + r.getDisk().getPersistence().getId());
                System.err.println("Path> " + r.getDisk().getVolume().getContainerPath());
                System.err.println("Path> " + r.getDisk().getVolume().getHostPath());
                for (Protos.Label label : r.getReservation().getLabels().getLabelsList()) {
                    System.err.println(label.getKey() + "=>" + label.getValue());
                } */
            } else if (r.getName().equals("gpus")) {
                gpu = (int) (r.getScalar().getValue());
            }
        }
        Resource ret = new Resource(cpu, memMB, diskMB, reservedDiskMB, gpu);
        ret.volumes().putAll(volumes);
        return ret;
    }
}
