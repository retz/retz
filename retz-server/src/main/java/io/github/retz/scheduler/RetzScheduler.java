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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.db.Database;
import io.github.retz.db.JobNotFoundException;
import io.github.retz.mesos.Resource;
import io.github.retz.mesos.ResourceConstructor;
import io.github.retz.protocol.StatusResponse;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.JobResult;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class RetzScheduler implements Scheduler {
    public static final String FRAMEWORK_NAME = "Retz-Framework";
    public static final String HTTP_SERVER_NAME;
    private static final Logger LOG = LoggerFactory.getLogger(RetzScheduler.class);

    static {
        // TODO: stop hard coding and get the file name in more generic way
        // COMMENT: I put the trick in build.gradle, saving the exact jar file name as resource bundle
        // REVIEW: http://www.eclipse.org/aether/ (not surveyed yet)
        // REVIEW: https://github.com/airlift/resolver (used in presto?)
        ResourceBundle labels = ResourceBundle.getBundle("retz-server");

        HTTP_SERVER_NAME = labels.getString("servername");
        LOG.info("Server name in HTTP(S) header: {}", HTTP_SERVER_NAME);
    }

    private final ObjectMapper MAPPER = new ObjectMapper();
    private final Map<String, Protos.Offer> OFFER_STOCK = new ConcurrentHashMap<>();
    private final Planner PLANNER = (Planner) new NaivePlanner();
    private final Protos.Filters filters = Protos.Filters.newBuilder().setRefuseSeconds(1).build();
    private Launcher.Configuration conf;
    private Protos.FrameworkInfo frameworkInfo;
    private Map<String, List<Protos.SlaveID>> slaves;

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
        // 'Framework has been removed' comes here;
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

        Optional<String> oldFrameworkId = Database.getInstance().getFrameworkId();
        if (oldFrameworkId.isPresent()) {
            if (oldFrameworkId.get().equals(frameworkId.getValue())) {
                // framework exists. nothing to do
                LOG.info("Framework id={} existed in past. Recovering any running jobs...", frameworkId.getValue());
                maybeRecoverRunning(driver);
            } else {
                LOG.error("Old different framework ({}) exists (!= {}). Quitting",
                        oldFrameworkId.get(), frameworkId.getValue());
                driver.stop();
            }
        } else {
            if (Database.getInstance().setFrameworkId(frameworkId.getValue())) {
            } else {
                LOG.warn("Failed to remember frameworkID...");
            }
        }
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
        LOG.info("Reconnected to master {}", masterInfo.getHostname());
        maybeRecoverRunning(driver);
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        LOG.debug("Resource offer: {}", offers.size());

        // Merge fresh offers from Mesos and offers in stock here, declining duplicate offers
        List<Protos.Offer> available = new LinkedList<>();
        synchronized (OFFER_STOCK) {
            // TODO: cleanup this code, optimize for max.stock = 0 case
            Map<String, List<Protos.Offer>> allOffers = new HashMap<>();
            for (Protos.Offer offer : OFFER_STOCK.values()) {
                String key = offer.getSlaveId().getValue();
                List<Protos.Offer> list = allOffers.getOrDefault(key, new LinkedList<>());
                list.add(offer);
                allOffers.put(offer.getSlaveId().getValue(), list);
            }
            for (Protos.Offer offer : offers) {
                String key = offer.getSlaveId().getValue();
                List<Protos.Offer> list = allOffers.getOrDefault(key, new LinkedList<>());
                list.add(offer);
                allOffers.put(offer.getSlaveId().getValue(), list);
            }

            int declined = 0;
            for (Map.Entry<String, List<Protos.Offer>> e : allOffers.entrySet()) {
                if (e.getValue().size() == 1) {
                    available.add(e.getValue().get(0));
                } else {
                    for (Protos.Offer dup : e.getValue()) {
                        driver.declineOffer(dup.getId(), filters);
                        declined += 1;
                    }
                }
            }
            if (conf.fileConfig.getMaxStockSize() > 0) {
                LOG.info("Offer stock renewal: {} offers available ({} declined from stock)", available.size(), declined);
            }
            OFFER_STOCK.clear();
        }

        Resource resource = new Resource(0, 0, 0);
        for (Protos.Offer offer : available) {
            LOG.debug("offer: {}", offer);
            resource.merge(ResourceConstructor.decode(offer.getResourcesList()));
        }

        List<Job> jobs = JobQueue.findFit((int) resource.cpu(), resource.memMB());
        handleAll(available, jobs, driver);
    }

    public void maybeInvokeNow(SchedulerDriver driver, Job job) {
        try {
            List<Job> queued = JobQueue.queued(1);
            if (queued.size() == 1 && queued.get(0).id() == job.id()) {
                // OK
            } else {
                return;
            }
        }catch (Exception e) {
            LOG.error("maybeInvokeNow failed: {}", e.toString());
            return;
        }

        List<Protos.Offer> available = new LinkedList<>();
        synchronized (OFFER_STOCK) {
            available.addAll(OFFER_STOCK.values());
            OFFER_STOCK.clear();
        }
        // Only if the queue is empty, and with offer stock, try job invocation
        List<Job> jobs = Arrays.asList(job);
        handleAll(available, jobs, driver);
    }

    public void handleAll(List<Protos.Offer> offers, List<Job> jobs, SchedulerDriver driver) {

        // TODO: this is fleaky limitation, build this into Planner.plan as a constraint
        // Check if simultaneous jobs exceeded its limit
        int running = JobQueue.countRunning();
        if (running >= conf.fileConfig.getMaxSimultaneousJobs()) {
            LOG.warn("Number of concurrently running jobs has reached its limit: {} >= {} ({})",
                    running, conf.fileConfig.getMaxSimultaneousJobs(), ServerConfiguration.MAX_SIMULTANEOUS_JOBS);
            return;
        }

        // DO MAKE PLANNING
        List<Job> cancel = new LinkedList<>();
        List<AppJobPair> appJobPairs = PLANNER.filter(jobs, cancel, conf.getServerConfig().useGPU());
        // update database to change all jobs state to KILLED
        JobQueue.cancelAll(cancel);

        Plan bestPlan = PLANNER.plan(offers, appJobPairs, conf.getServerConfig().getMaxStockSize());

        // Update local database, to running
        for (Job j : bestPlan.getToBeLaunched()) {
            JobQueue.starting(j, Optional.empty(), j.taskId());
        }
        // Accept offers to mesos
        if (!(bestPlan.getToBeAccepted().isEmpty() && bestPlan.getOperations().isEmpty())) {
            driver.acceptOffers(bestPlan.getToBeAccepted(), bestPlan.getOperations(), filters);
        }

        // update database to change all jobs state to KILLED
        JobQueue.cancelAll(bestPlan.getToCancel());

        // Stock unused offers, return duplicate offers
        OFFER_STOCK.putAll(bestPlan.getToStock());
        for (Protos.OfferID id : bestPlan.getToDecline()) {
            driver.declineOffer(id, filters);
        }
        LOG.info("{} accepted, {} declined ({} offers back in stock)",
                bestPlan.getToBeAccepted().size(), bestPlan.getToDecline().size(), bestPlan.getToStock().size());
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
        try {
            JobQueue.retry(status.getTaskId().getValue(), reason);
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        } catch (JobNotFoundException e) {
            LOG.warn(e.toString(), e);
            // TODO: re-insert the failed job again?
        }
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
        try {
            JobQueue.finished(status.getTaskId().getValue(), maybeUrl, ret, finished);
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        } catch (JobNotFoundException e) {
            LOG.warn(e.toString(), e);
            // TODO: re-insert the failed job again?
        }

    }

    void failed(Protos.TaskStatus status) {
        Optional<String> maybeUrl = MesosHTTPFetcher.sandboxBaseUri(conf.getMesosMaster(),
                status.getSlaveId().getValue(), frameworkInfo.getId().getValue(),
                status.getExecutorId().getValue());
        try {
            JobQueue.failed(status.getTaskId().getValue(), maybeUrl, status.getMessage());
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        } catch (JobNotFoundException e) {
            LOG.warn(e.toString(), e);
            // TODO: re-insert the failed job again?
        }
    }

    void started(Protos.TaskStatus status) {
        Optional<String> maybeUrl = MesosHTTPFetcher.sandboxBaseUri(conf.getMesosMaster(),
                status.getSlaveId().getValue(), frameworkInfo.getId().getValue(),
                status.getExecutorId().getValue());
        try {
            JobQueue.started(status.getTaskId().getValue(), maybeUrl);
        } catch (SQLException e) {
            LOG.error(e.toString(), e);
        } catch (JobNotFoundException e) {
            LOG.warn(e.toString(), e);
            // TODO: re-insert the failed job again?
        } catch (IOException e) {
            LOG.error(e.toString(), e);
        }
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

    // Get all running jobs and sync its latest state in Mesos
    // If it's not lost, just update state. Otherwise, set its state as QUEUED back.
    // TODO: offload this from scheduler callback thread
    private void maybeRecoverRunning(SchedulerDriver driver) {
        List<Job> jobs = Database.getInstance().getRunning();
        Database.getInstance().retryJobs(jobs.stream().map(job -> job.id()).collect(Collectors.toList()));
    }


}
