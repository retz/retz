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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.cli.FileConfiguration;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.mesos.Resource;
import io.github.retz.mesos.ResourceConstructor;
import io.github.retz.protocol.StatusResponse;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.JobResult;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class RetzScheduler implements Scheduler {
    public static final String FRAMEWORK_NAME = "Retz-Framework";
    private static final Logger LOG = LoggerFactory.getLogger(RetzScheduler.class);
    private Launcher.Configuration conf;
    private Protos.FrameworkInfo frameworkInfo;
    private Map<String, List<Protos.SlaveID>> slaves;
    private final ObjectMapper MAPPER = new ObjectMapper();
    public static final String HTTP_SERVER_NAME;

    private final Map<String, Protos.Offer> OFFER_STOCK = new ConcurrentHashMap<>();

    static {
        // TODO: stop hard coding and get the file name in more generic way
        // COMMENT: I put the trick in build.gradle, saving the exact jar file name as resource bundle
        // REVIEW: http://www.eclipse.org/aether/ (not surveyed yet)
        // REVIEW: https://github.com/airlift/resolver (used in presto?)
        ResourceBundle labels = ResourceBundle.getBundle("retz-server");

        HTTP_SERVER_NAME = labels.getString("servername");
        LOG.info("Server name in HTTP(S) header: {}", HTTP_SERVER_NAME);
    }

    public RetzScheduler(Launcher.Configuration conf, Protos.FrameworkInfo frameworkInfo) {
        MAPPER.registerModule(new Jdk8Module());
        this.conf = Objects.requireNonNull(conf);
        this.frameworkInfo = frameworkInfo;
        this.slaves = new ConcurrentHashMap<>();

        for (Map.Entry<String, Protos.Offer> e : OFFER_STOCK.entrySet()) {
            OFFER_STOCK.remove(e.getKey());
        }
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
        LOG.info("Offer rescinded: {}", offerId.getValue());
        OFFER_STOCK.remove(offerId.getValue());
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
            // Check if simultaneous jobs exceeded its limit
            //int running = JobQueue.getRunning().size();
            int running = JobQueue.countRunning();
            if (running >= conf.fileConfig.getMaxSimultaneousJobs()) {
                LOG.warn("Number of concurrently running jobs has reached its limit: {} >= {} ({})",
                        running, conf.fileConfig.getMaxSimultaneousJobs(), FileConfiguration.MAX_SIMULTANEOUS_JOBS);
                break;
            }

            Resource resource = ResourceConstructor.decode(offer.getResourcesList());
            LOG.info("Offer {}: {}", offer.getId().getValue(), resource);

            List<Protos.Offer.Operation> operations = new ArrayList<>();

            operations.addAll(Applications.persistentVolumeOps(resource, frameworkInfo));

            // Cleanup unused spaces, volumes
            operations.addAll(Applications.persistentVolumeCleanupOps(resource, frameworkInfo));

            List<Job> jobs = JobQueue.findFit((int) resource.cpu(), resource.memMB());

            LOG.debug("{} jobs found for fit {}/{}", jobs.size(), resource.cpu(), resource.memMB());

            handleOffer(driver, resource, offer, jobs, operations, true);
        }
    }

    public void maybeInvokeNow(SchedulerDriver driver, Job job) {
        List<String> used = new LinkedList<>();
        for (Map.Entry<String, Protos.Offer> pair : OFFER_STOCK.entrySet()) {
            Protos.Offer offer = pair.getValue();
            Resource resource = ResourceConstructor.decode(offer.getResourcesList());
            List<Protos.Offer.Operation> ops = new LinkedList<>();
            Job[] jobs = {job};
            if (handleOffer(driver, resource, offer, Arrays.asList(jobs), ops, false) == 1) {
                used.add(pair.getKey());
                break;
            }
        }
        for (String key : used) {
            OFFER_STOCK.remove(key);
        }
    }

    private synchronized int handleOffer(SchedulerDriver driver, Resource resource, Protos.Offer offer, List<Job> jobs,
                                         List<Protos.Offer.Operation> operations, boolean doStock) {
        // Assign tasks if it has enough CPU/Memory
        Resource assigned = new Resource(0, 0, 0);

        List<AppJob> appJobs = jobs.stream().filter(job -> {
            if (!conf.fileConfig.useGPU() && job.gpu() > 0) {
                // TODO: this should be checked before enqueuing to JobQueue
                String reason = String.format("Job (%d@%s) requires %d GPUs while this Retz Scheduler is not capable of using GPU resources. Try setting retz.gpu=true at retz.properties.",
                        job.id(), job.appid(), job.gpu());
                //kill(job, reason);
                JobQueue.cancel(job.id(), reason);
                return false;
            }
            return true;
        }).map(job -> {
            Optional<Application> app = Applications.get(job.appid());
            return new AppJob(app, job);
        }).filter(appJob -> {
            if (!appJob.hasApplication()) {
                String reason = String.format("Application %s does not exist any more. Cancel.", appJob.job().appid());
                JobQueue.cancel(appJob.job().id(), reason);
                return false;
            }
            return true;
        }).collect(Collectors.toList());

        for (AppJob appJob : appJobs) {
            Job job = appJob.job();
            Application app = appJob.application().get();
            String id = Integer.toString(job.id());
            // Not using simple CommandExecutor to keep the executor lifecycle with its assets
            // (esp ASAKUSA_HOME env)
            TaskBuilder tb = new TaskBuilder(conf.getFileConfig().getMesosAgentJava())
                    .setOffer(resource, job.cpu(), job.memMB(), job.gpu(), offer.getSlaveId())
                    .setName("retz-task-name-" + job.name())
                    .setTaskId("retz-task-id-" + id)
                    .setCommand(job, app);

            String volumeId = app.toVolumeId(frameworkInfo.getRole());
            if (app.getDiskMB().isPresent() && resource.volumes().containsKey(volumeId)) {
                LOG.debug("getting {}, diskMB {}", volumeId, app.getDiskMB().get());
                Protos.Resource res = resource.volumes().get(volumeId);
                LOG.debug("name:{}, role:{}, path:{}, size:{}, principal:{}",
                        res.getName(), res.getRole(), res.getDisk().getVolume().getContainerPath(),
                        res.getScalar().getValue(),
                        res.getReservation().getPrincipal());
                tb.setVolume(resource, volumeId);
            }

            assigned.merge(tb.getAssigned());
            Protos.TaskInfo task = tb.build();

            Protos.Offer.Operation.Launch launch = Protos.Offer.Operation.Launch.newBuilder()
                    .addTaskInfos(Protos.TaskInfo.newBuilder(task))
                    .build();

            Protos.TaskID taskId = task.getTaskId();
            //job.starting(TimestampHelper.now());

            // This shouldn't be a List nor a set, but a Map
            List<Protos.SlaveID> slaves = this.slaves.getOrDefault(app.getAppid(), new LinkedList<>());
            slaves.add(offer.getSlaveId());
            this.slaves.put(app.getAppid(), slaves);

            JobQueue.starting(job, Optional.empty(), taskId.getValue());

            operations.add(Protos.Offer.Operation.newBuilder()
                    .setType(Protos.Offer.Operation.Type.LAUNCH)
                    .setLaunch(launch).build());
            LOG.info("Job {}(task {}) is to be ran as '{}' with {} at Slave {}",
                    job.id(), taskId.getValue(), job.cmd(), tb.getAssigned().toString(), offer.getSlaveId().getValue());

        }

        if (operations.isEmpty()) {
            // Clean up with existing stock if any duplication found
            List<Protos.OfferID> dups = new LinkedList<>();
            for (Map.Entry<String, Protos.Offer> e : OFFER_STOCK.entrySet()) {
                if (e.getValue().getSlaveId().getValue().equals(offer.getSlaveId().getValue())) {
                    // Duplicate!
                    dups.add(e.getValue().getId());
                }
            }
            if (!dups.isEmpty()) {
                LOG.info("Discarding {} partial offers from same agent", dups.size() + 1);
                for (Protos.OfferID oid : dups) {
                    OFFER_STOCK.remove(oid.getValue());
                    driver.declineOffer(oid);
                }
                driver.declineOffer(offer.getId());
            } else if (doStock) {
                if (OFFER_STOCK.size() < conf.getFileConfig().getMaxStockSize()) {
                    LOG.info("Stocking offer {}", offer.getId().getValue());
                    OFFER_STOCK.put(offer.getId().getValue(), offer);
                } else {
                    LOG.debug("Nothing to do: declining the whole offer");
                    driver.declineOffer(offer.getId());
                }
            }
        } else {
            Protos.Filters filters = Protos.Filters.newBuilder().setRefuseSeconds(1).build();
            List<Protos.OfferID> offerIds = new ArrayList<>();
            offerIds.add(offer.getId());
            LOG.info("Total resource newly used: {}", assigned.toString());
            LOG.info("Accepting offer {}, {} operations (remaining: {})", offer.getId().getValue(), operations.size(), resource);
            driver.acceptOffers(offerIds, operations, filters);
        }
        return operations.size();
    }

    @Override
    public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId,
                             int status) {
        LOG.info("Executor {} of slave {}  stopped: {}", executorId.getValue(), slaveId.getValue(), status);

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
        //JobQueue.recoverRunning();
        // TODO: reimplement with RDBMS clean query
        // throw new AssertionError("Not implemented yet");
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        LOG.info("Status update of task {}: {} / {}", status.getTaskId().getValue(), status.getState().name(), status.getMessage());

        switch (status.getState().getNumber()) {
            case Protos.TaskState.TASK_FINISHED_VALUE: {
                finished(status);
                break;
            }
            case Protos.TaskState.TASK_ERROR_VALUE:
            case Protos.TaskState.TASK_FAILED_VALUE:
            case Protos.TaskState.TASK_KILLED_VALUE: {
                failed(status);
                break;
            }
            case Protos.TaskState.TASK_LOST_VALUE: {
                retry(status);
                break;
            }
            case Protos.TaskState.TASK_KILLING_VALUE:
                break;
            case Protos.TaskState.TASK_RUNNING_VALUE:
                started(status);
                break;
            case Protos.TaskState.TASK_STAGING_VALUE:
                break;
            case Protos.TaskState.TASK_STARTING_VALUE:
                LOG.debug("Task {} starting", status.getTaskId().getValue());
                Optional<Job> job = JobQueue.getFromTaskId(status.getTaskId().getValue());
                if (job.isPresent()) {
                    JobQueue.starting(job.get(), MesosHTTPFetcher.sandboxBaseUri(conf.getMesosMaster(),
                            status.getSlaveId().getValue(), frameworkInfo.getId().getValue(),
                            status.getExecutorId().getValue()), status.getTaskId().getValue());
                }
                break;
            default:
                break;
        }
    }

    // Maybe Retry
    void retry(Protos.TaskStatus status) {
        String reason = "";
        if (status.hasData()) {
            try {
                JobResult jobResult = MAPPER.readValue(status.getData().toByteArray(), JobResult.class);
                reason = jobResult.reason();
            } catch (IOException e) {
            }
        }
        JobQueue.retry(status.getTaskId().getValue(), reason);
    }

    void finished(Protos.TaskStatus status) {
        Optional<String> maybeUrl = MesosHTTPFetcher.sandboxBaseUri(conf.getMesosMaster(),
                status.getSlaveId().getValue(), frameworkInfo.getId().getValue(),
                status.getExecutorId().getValue());

        int ret = status.getState().getNumber() - Protos.TaskState.TASK_FINISHED_VALUE;
        String finished = TimestampHelper.now();
        if (status.hasData()) {
            try {
                JobResult jobResult = MAPPER.readValue(status.getData().toByteArray(), JobResult.class);
                ret = jobResult.result();
                finished = jobResult.finished();
            } catch (IOException e) {
                LOG.error("Exception: {}", e.toString());
            }
        }
        JobQueue.finished(status.getTaskId().getValue(), maybeUrl, ret, finished);
        // notify watchers?
    }

    void failed(Protos.TaskStatus status) {
        Optional<String> maybeUrl = MesosHTTPFetcher.sandboxBaseUri(conf.getMesosMaster(),
                status.getSlaveId().getValue(), frameworkInfo.getId().getValue(),
                status.getExecutorId().getValue());

        JobQueue.failed(status.getTaskId().getValue(), maybeUrl, status.getMessage());
    }

    void started(Protos.TaskStatus status) {
        Optional<String> maybeUrl = MesosHTTPFetcher.sandboxBaseUri(conf.getMesosMaster(),
                status.getSlaveId().getValue(), frameworkInfo.getId().getValue(),
                status.getExecutorId().getValue());

        JobQueue.started(status.getTaskId().getValue(), maybeUrl);
        //WebConsole.notifyStarted(job);
    }

    public void setOfferStats(StatusResponse statusResponse) {
        int totalCpu = 0;
        int totalMem = 0;
        int totalGpu = 0;
        for (Map.Entry<String, Protos.Offer> e : OFFER_STOCK.entrySet()) {
            Resource r = ResourceConstructor.decode(e.getValue().getResourcesList());
            totalCpu += r.cpu();
            totalMem += r.memMB();
            totalGpu += r.gpu();
        }
        statusResponse.setOfferStats(OFFER_STOCK.size(), totalCpu, totalMem, totalGpu);
    }

    private static class AppJob {
        private final Optional<Application> app;
        private final Job job;

        AppJob(Optional<Application> a, Job j) {
            app = a;
            job = j;
        }

        boolean hasApplication() {
            return app.isPresent();
        }

        Optional<Application> application() {
            return app;
        }

        Job job() {
            return job;
        }
    }
}
