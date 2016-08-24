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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.mesos.Resource;
import io.github.retz.mesos.ResourceConstructor;
import io.github.retz.protocol.Job;
import io.github.retz.web.WebConsole;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class RetzScheduler implements Scheduler {
    public static final String FRAMEWORK_NAME = "Retz-Framework";
    private static final Logger LOG = LoggerFactory.getLogger(RetzScheduler.class);
    private static String jarUri;
    private MesosFrameworkLauncher.Configuration conf;
    private Protos.FrameworkInfo frameworkInfo;
    private Map<String, List<Protos.SlaveID>> slaves;

    public RetzScheduler(MesosFrameworkLauncher.Configuration conf, Protos.FrameworkInfo frameworkInfo) {
        this.conf = Objects.requireNonNull(conf);
        this.frameworkInfo = frameworkInfo;
        this.slaves = new ConcurrentHashMap<>();

        // TODO: stop hard coding and get the file name in more generic way
        // COMMENT: I put the trick in build.gradle, saving the exact jar file name as resource bundle
        // REVIEW: http://www.eclipse.org/aether/ (not surveyed yet)
        // REVIEW: https://github.com/airlift/resolver (used in presto?)
        ResourceBundle labels = ResourceBundle.getBundle("ExecutorJarFile");
        String filename = labels.getString("filename");
        LOG.info("Executor jar file name to distribute: {}", filename);
        RetzScheduler.setJarUri(conf.fileConfig.getUri() + "/" + filename);
    }

    public static String getJarUri() {
        return jarUri;
    }

    public static void setJarUri(String uri) {
        LOG.info("Executor jar distribution URI: {}", uri);
        jarUri = uri;
    }

    public void stopAllExecutors(SchedulerDriver driver, String appName) {
        List<Protos.SlaveID> slaves = this.slaves.get(appName);
        Protos.ExecutorID executorID = Protos.ExecutorID.newBuilder().setValue(appName).build();
        byte[] msg = {'s', 't', 'o', 'p'};
        if (slaves != null) {
            for (Protos.SlaveID slave : slaves) {
                driver.sendFrameworkMessage(executorID, slave, msg);
            }
        }
    }

    @Override
    public void disconnected(SchedulerDriver driver) {
        LOG.warn("Disconnected from cluster");
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
        LOG.error(message);
    }


    @Override
    public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {
        LOG.info("Framework Message ({} bytes)", data.length);
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
        LOG.warn("Offer rescinded: {}", offerId.getValue());
    }

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
        LOG.info("Connected to master {}; Framework ID: {}", masterInfo.getHostname(), frameworkId.getValue());
        frameworkInfo = frameworkInfo.toBuilder().setId(frameworkId).build();
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
        LOG.info("Connected to master {}", masterInfo.getHostname());
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        LOG.debug("Resource offer: {}", offers.size());

        // TODO TODO TODO: much more refinment on assigning resources

        for (Protos.Offer offer : offers) {

            Resource r = ResourceConstructor.decode(offer.getResourcesList());
            LOG.debug("Offer: {}", r);

            List<Protos.Offer.Operation> operations = new ArrayList<>();

            SetMap commands = new SetMap();
            // REVIEW: This "resource" object is equivalent to "r" above
            Resource resource = ResourceConstructor.decode(offer.getResourcesList());

            List<Applications.Application> apps = Applications.needsPersistentVolume(resource, frameworkInfo.getRole());
            // REVIEW: These logs are debug level
            for (Applications.Application a : apps) {
                LOG.info("Application {}: {} {} volume: {} {}MB",
                        a.appName, String.join(" ", a.persistentFiles),
                        String.join(" ", a.appFiles), a.toVolumeId(frameworkInfo.getRole()), a.diskMB);
            }
            int reservedSpace = resource.reservedDiskMB();
            LOG.debug("Number of volumes: {}, reserved space: {}", r.volumes().size(), reservedSpace);

            for (Applications.Application app : apps) {

                String volumeId = app.toVolumeId(frameworkInfo.getRole());
                LOG.info("Application {} needs {} MB persistent volume", app.appName, app.diskMB);
                if (reservedSpace < app.diskMB.get()) {
                    // If enough space is not reserved, then reserve
                    // TODO: how it must behave when we only have too few space to reserve
                    Protos.Resource.Builder rb = baseResourceBuilder(app.diskMB.get() - reservedSpace);

                    LOG.info("Reserving resource with principal={}, role={}", frameworkInfo.getPrincipal(), frameworkInfo.getRole());
                    operations.add(Protos.Offer.Operation.newBuilder()
                            .setType(Protos.Offer.Operation.Type.RESERVE)
                            .setReserve(Protos.Offer.Operation.Reserve.newBuilder()
                                    .addResources(rb.build())
                            )
                            .build());
                } else if (!r.volumes().containsKey(volumeId)) {
                    // We have enough space to reserve, then do it
                    Protos.Resource.Builder rb = baseResourceBuilder(app.diskMB.get());
                    LOG.info("Creating volume {} with {}", volumeId, app.appName);
                    operations.add(Protos.Offer.Operation.newBuilder()
                            .setType(Protos.Offer.Operation.Type.CREATE)
                            .setCreate(Protos.Offer.Operation.Create.newBuilder().addVolumes(
                                    rb.setDisk(Protos.Resource.DiskInfo.newBuilder()
                                            .setPersistence(Protos.Resource.DiskInfo.Persistence.newBuilder()
                                                    .setPrincipal(frameworkInfo.getPrincipal())
                                                    .setId(volumeId))
                                            .setVolume(Protos.Volume.newBuilder()
                                                    .setMode(Protos.Volume.Mode.RW)
                                                    .setContainerPath(app.appName + "-home")))
                                            .build())
                            ).build());
                    reservedSpace -= app.diskMB.get();
                }
            }

            // Cleanup unused spaces, volumes
            List<String> usedVolumes = Applications.volumes(frameworkInfo.getRole());
            Map<String, Protos.Resource> unusedVolumes = new LinkedHashMap<>();
            for (Map.Entry<String, Protos.Resource> volume : resource.volumes().entrySet()) {
                if (!usedVolumes.contains(volume.getKey())) {
                    unusedVolumes.put(volume.getKey(), volume.getValue());
                }
            }
            for (Map.Entry<String, Protos.Resource> volume : unusedVolumes.entrySet()) {
                String volumeId = volume.getKey();
                LOG.info("Destroying {}", volumeId);
                operations.add(Protos.Offer.Operation.newBuilder()
                        .setType(Protos.Offer.Operation.Type.DESTROY)
                        .setDestroy(Protos.Offer.Operation.Destroy.newBuilder().addVolumes(volume.getValue()))
                        .build());
                LOG.info("Unreserving {}", volumeId);
                operations.add(Protos.Offer.Operation.newBuilder()
                        .setType(Protos.Offer.Operation.Type.UNRESERVE)
                        .setUnreserve(Protos.Offer.Operation.Unreserve.newBuilder()
                                .addResources(Protos.Resource.newBuilder()
                                        .setReservation(Protos.Resource.ReservationInfo.newBuilder()
                                                .setPrincipal(frameworkInfo.getPrincipal()))
                                        .setName("disk")
                                        .setType(Protos.Value.Type.SCALAR)
                                        .setScalar(resource.volumes().get(volumeId).getScalar())
                                        .setRole(frameworkInfo.getRole())
                                        .build())
                        ).build());
            }

            Resource assigned = new Resource(0, 0, 0);
            List<Job> jobs = JobQueue.popMany((int) r.cpu(), r.memMB());
            // Assign tasks if it has enough CPU/Memory
            for (Job job : jobs) {
                Optional<Applications.Application> app = Applications.get(job.appid());

                if (!app.isPresent()) {
                    String reason = String.format("Application %s does not exist any more. Cancel.", job.appid());
                    kill(job, reason);
                    continue;
                }
                if (commands.hasEntry(job.appid(), job.cmd())) {
                    String reason = String.format("Same command is going to be scheduled in single executor; skipping %s '%s'",
                            job.appid(), job.cmd());
                    kill(job, reason);
                    continue;
                } else if (!conf.fileConfig.useGPU() && job.gpu().getMin() > 0) {
                    // TODO: this should be checked before enqueuing to JobQueue
                    String reason = String.format("Job (%d@%s) requires %d GPUs while this Retz Scheduler is not capable of using GPU resources. Try setting retz.gpu=true at retz.properties.",
                            job.id(), job.appid(), job.gpu().getMin());
                    kill(job, reason);
                    continue;
                } else {
                    commands.add(job.appid(), job.cmd());
                }


                String id = Integer.toString(job.id());
                // Not using simple CommandExecutor to keep the executor lifecycle with its assets
                // (esp ASAKUSA_HOME env)
                Protos.ExecutorInfo executorInfo = app.get().toExecutorInfo(frameworkInfo.getId());
                try {
                    TaskBuilder tb = new TaskBuilder()
                            .setOffer(r, job.cpu(), job.memMB(), job.gpu(), offer.getSlaveId())
                            .setName("retz-task-name-" + job.name())
                            .setTaskId("retz-task-id-" + id)
                            .setExecutor(executorInfo)
                            .setJob(job, Applications.encodable(app.get()));

                    String volumeId = app.get().toVolumeId(frameworkInfo.getRole());
                    if (app.get().diskMB.isPresent() && resource.volumes().containsKey(volumeId)) {
                        LOG.info("getting {}, diskMB {}", volumeId, app.get().diskMB.get());
                        Protos.Resource res = r.volumes().get(volumeId);
                        LOG.debug("name:{}, role:{}, path:{}, size:{}, principal:{}",
                                res.getName(), res.getRole(), res.getDisk().getVolume().getContainerPath(),
                                res.getScalar().getValue(),
                                res.getReservation().getPrincipal());
                        tb.setVolume(r, volumeId);
                    }

                    assigned.merge(tb.getAssigned());
                    Protos.TaskInfo task = tb.build();

                    Protos.Offer.Operation.Launch launch = Protos.Offer.Operation.Launch.newBuilder()
                            .addTaskInfos(Protos.TaskInfo.newBuilder(task))
                            .build();

                    Protos.TaskID taskId = task.getTaskId();
                    job.setStarted(TimestampHelper.now());
                    WebConsole.notifyStarted(job);

                    // This shouldn't be a List nor a set, but a Map
                    List<Protos.SlaveID> slaves = this.slaves.getOrDefault(app.get().appName, new LinkedList<>());
                    slaves.add(offer.getSlaveId());
                    this.slaves.put(app.get().appName, slaves);

                    JobQueue.start(taskId.getValue(), job);

                    operations.add(Protos.Offer.Operation.newBuilder()
                            .setType(Protos.Offer.Operation.Type.LAUNCH)
                            .setLaunch(launch).build());

                    LOG.info("Task {} is to be ran as '{}' with {}", taskId.getValue(), job.cmd(), tb.getAssigned().toString());

                } catch (JsonProcessingException e) {
                    // REVIEW: It seems that JsonProcessingException may be thrown only from TaskBuilder#setJob.
                    // Is it reasonable to move this catch clause just after it and narrow the try scope?
                    String reason = String.format("Cannot encode job to JSON: %s - killing the job", job);
                    kill(job, reason);
                }
            }
            if (jobs.isEmpty() && operations.isEmpty()) {
                LOG.debug("Nothing to do: declining the whole offer");
                driver.declineOffer(offer.getId());
            } else {
                Protos.Filters filters = Protos.Filters.newBuilder().setRefuseSeconds(1).build();
                List<Protos.OfferID> offerIds = new ArrayList<>();
                offerIds.add(offer.getId());
                LOG.info("Total resource newly used: {}", assigned.toString());
                LOG.info("Accepting offer {}, {} operations (remaining: {})", offer.getId().getValue(), operations.size(), r);
                driver.acceptOffers(offerIds, operations, filters);
            }
        }
    }

    @Override
    public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId,
                             int status) {
        LOG.info("Executor stopped: {}", executorId.getValue());

        // TODO: do we really need to manage slaves?
        List<Protos.SlaveID> slaves = this.slaves.get(executorId.getValue());
        if (slaves != null) {
            slaves.remove(slaveId);
            this.slaves.put(executorId.getValue(), slaves);
        }
    }

    // @doc Re-schedule **all** running job when a Slave is lost. I know it's a kludge.
    @Override
    public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
        LOG.warn("Slave lost: {}", slaveId.getValue());
        for (Map.Entry<String, List<Protos.SlaveID>> entry : slaves.entrySet()) {
            List<Protos.SlaveID> list = entry.getValue();
            for (Protos.SlaveID s : list) {
                if (s.getValue().equals(slaveId.getValue())) {
                    list.remove(s);
                }
            }
            slaves.put(entry.getKey(), list);
        }

        // TODO: remove **ONLY** tasks that is running on the failed slave
        // REVIEW: :+1:
        JobQueue.recoverRunning();
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        LOG.info("Status update of task {}: {}", status.getTaskId().getValue(), status.getState().name());

        switch (status.getState().getNumber()) {
            case Protos.TaskState.TASK_FINISHED_VALUE: {
                finished(status);
                break;
            }
            case Protos.TaskState.TASK_ERROR_VALUE:
            case Protos.TaskState.TASK_FAILED_VALUE:
            case Protos.TaskState.TASK_KILLED_VALUE:
            case Protos.TaskState.TASK_KILLING_VALUE:
            case Protos.TaskState.TASK_LOST_VALUE: {
                failed(status);
                break;
            }
            case Protos.TaskState.TASK_RUNNING_VALUE:
                break;
            case Protos.TaskState.TASK_STAGING_VALUE:
                break;
            case Protos.TaskState.TASK_STARTING_VALUE:
                LOG.debug("Task {} starting", status.getTaskId().getValue());
                break;
            default:
                break;
        }
    }

    void finished(Protos.TaskStatus status) {
        Job job = JobQueue.finish(status.getTaskId().getValue());
        int result = -42;
        try {
            result = Integer.parseInt(status.getMessage());
            job.finished(MesosHTTPFetcher.sandboxBaseUri(conf.getMesosMaster(),
                    status.getSlaveId().getValue(), frameworkInfo.getId().getValue(),
                    status.getExecutorId().getValue()).get(), TimestampHelper.now(), result);
            LOG.info("Job {} (id {}) has finished. State: {}",
                    status.getTaskId().getValue(), job.id(), status.getState().name());
            WebConsole.notifyFinished(job);
        } catch (Exception e) {
            String msg = String.format("Failed to parse message from executor: %s '%s'", e.toString(), status.getMessage());
            LOG.warn(msg);
            job.killed(TimestampHelper.now(), msg);
            WebConsole.notifyKilled(job);
        }
    }
    void failed(Protos.TaskStatus status) {

        Job job = JobQueue.finish(status.getTaskId().getValue());
        int result = -42;
        try {
            result = Integer.parseInt(status.getMessage());
            job.finished(MesosHTTPFetcher.sandboxBaseUri(conf.getMesosMaster(),
                    status.getSlaveId().getValue(), frameworkInfo.getId().getValue(),
                    status.getExecutorId().getValue()).get(), TimestampHelper.now(), result);
            LOG.info("Job {} (id {}) has been failed. State: {}",
                    status.getTaskId().getValue(), job.id(), status.getState().name());
            job.killed(TimestampHelper.now(), status.getState().name());
            WebConsole.notifyKilled(job);

        } catch (NumberFormatException e) {
            String msg = String.format("Failed to parse message from executor: '%s'", status.getMessage());
            LOG.warn(msg);
            LOG.warn("Exception: {}", e.toString());
            job.killed(TimestampHelper.now(), msg);
            WebConsole.notifyKilled(job);
        }
    }
    void kill(Job job, String reason) {
        LOG.warn(reason);
        job.killed(TimestampHelper.now(), reason);
        JobQueue.kill(job);
        WebConsole.notifyKilled(job);
    }


    private Protos.Resource.Builder baseResourceBuilder(int diskMB) {
        return Protos.Resource.newBuilder()
                .setName("disk")
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(diskMB))
                .setType(Protos.Value.Type.SCALAR)
                .setReservation(Protos.Resource.ReservationInfo.newBuilder()
                        .setPrincipal(frameworkInfo.getPrincipal()))
                .setRole(frameworkInfo.getRole());
    }

    private static class SetMap {
        private final Map<String, Set<String>> map;

        SetMap() {
            map = new HashMap<>();
        }

        boolean hasEntry(String key, String element) {
            return map.containsKey(key) &&
                    map.get(key).contains(element);
        }

        void add(String key, String element) {
            if (map.containsKey(key)) {
                map.get(key).add(element);
            } else {
                Set<String> set = new HashSet<>();
                set.add(element);
                map.put(key, set);
            }
        }
    }
}
