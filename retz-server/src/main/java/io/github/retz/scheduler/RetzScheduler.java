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
package io.github.retz.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.db.Database;
import io.github.retz.mesosc.MesosHTTPFetcher;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.ResourceQuantity;
import io.github.retz.protocol.exception.JobNotFoundException;
import io.github.retz.web.StatusCache;
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

    private final ResourceQuantity MAX_JOB_SIZE;
    private final ObjectMapper MAPPER = new ObjectMapper();
    private final Map<String, Protos.Offer> OFFER_STOCK = new ConcurrentHashMap<>();
    private final Planner PLANNER;
    private final Protos.Filters filters;
    private Launcher.Configuration conf;
    private Protos.FrameworkInfo frameworkInfo;
    private Map<String, List<Protos.SlaveID>> slaves;

    public RetzScheduler(Launcher.Configuration conf, Protos.FrameworkInfo frameworkInfo) {
        MAPPER.registerModule(new Jdk8Module());
        PLANNER = PlannerFactory.create(conf.getServerConfig().getPlannerName());
        this.conf = Objects.requireNonNull(conf);
        this.frameworkInfo = frameworkInfo;
        this.slaves = new ConcurrentHashMap<>();
        this.filters = Protos.Filters.newBuilder().setRefuseSeconds(conf.getServerConfig().getRefuseSeconds()).build();
        MAX_JOB_SIZE = conf.getServerConfig().getMaxJobSize();
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

    // There is a potential race between offerRescinded and using offer stocks;
    // in case handleAll trying to schedule tasks, offers are removed from OFFER_STOCK
    // but being used to schedule tasks - this message can't be in time ...
    // The task with rescinded offer (slave) might fail in advance; TASK_FAILED or TASK_LOST?
    // any way in this case it should be retried...
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
        // Maybe long time split brain, recovering all states from master required.
        maybeRecoverRunning(driver);
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        LOG.debug("Resource offer: {}", offers.size());

        // Merge fresh offers from Mesos and offers in stock here, declining duplicate offers
        Stanchion.schedule(() -> {
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

            ResourceQuantity total = new ResourceQuantity();
            for (Protos.Offer offer : available) {
                LOG.debug("offer: {}", offer);
                Resource resource = ResourceConstructor.decode(offer.getResourcesList());
                total.add(resource.toQuantity());
            }
            total.setNodes(offers.size());

            // TODO: change findFit to consider not only CPU and Memory, but GPUs and Ports
            List<Job> jobs = JobQueue.findFit(PLANNER.orderBy(), total);
            handleAll(available, jobs, driver);
            // As this section is whole serialized by Stanchion, it is safe to do fetching jobs
            // from database and updating database state change from queued => starting at
            // separate transactions
        });
    }

    public void maybeInvokeNow(SchedulerDriver driver, Job job) {
        Stanchion.schedule(() -> {
            try {
                List<Job> queued = JobQueue.queued(1);
                if (queued.size() == 1 && queued.get(0).id() == job.id()) {
                    // OK
                } else {
                    return;
                }
            } catch (Exception e) {
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
        });
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

        // TODO: split pure-planning code and Mesos-related code; don't create TaskInfo and Launches here
        // TODO: unix user name is used for TaskInfo setup and not related to pure planning.
        // FIXME: TODO: this↑ is definitely a tech debt!
        Plan bestPlan = PLANNER.plan(offers, appJobPairs, conf.getServerConfig().getMaxStockSize(), conf.getServerConfig().getUserName());

        int declined = 0;
        // Accept offers from mesos
        for (OfferAcceptor acceptor : bestPlan.getOfferAcceptors()) {
            if (acceptor.getJobs().isEmpty()) {
                declined += acceptor.declineOffer(driver, filters);
            } else {
                for (Job j : acceptor.getJobs()) {
                    // Update local database, to running
                    JobQueue.starting(j, Optional.empty(), j.taskId());
                }
                acceptor.acceptOffers(driver, filters);
            }
        }
        for (Protos.Offer offer : bestPlan.getToStock()) {
            OFFER_STOCK.put(offer.getSlaveId().getValue(), offer);
        }
        LOG.info("{} accepted, {} declined ({} offers back in stock)",
                bestPlan.getOfferAcceptors().stream().mapToInt(offerAcceptor -> offerAcceptor.getJobs().size()).sum(),
                declined, bestPlan.getToStock().size());

        updateOfferStats();
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
        Stanchion.schedule(() -> maybeRecoverRunning(driver));
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        LOG.info("Status update of task {}: {} / {}", status.getTaskId().getValue(), status.getState().name(), status.getMessage());

        /**
         *
         *  Events   \ in Retz    | QUEUED   | STARTING | STARTED  | FINISHED | KILLED
         * -----------------------+----------+----------+----------+----------+----------
         *   TASK_FINISHED_VALUE: | finished | finished | finished | finished | finished
         *   TASK_ERROR_VALUE:    | failed   | failed   | failed   | failed   | failed
         *   TASK_FAILED_VALUE:   | ^^       | ^^       | ^^       | ^^       | ^^
         *   TASK_KILLED_VALUE:   | ^^       | ^^       | ^^       | ^^       | ^^
         *   TASK_LOST_VALUE:     | retry    | retry    | retry    | retry    | retry
         *   TASK_KILLING_VALUE:  | noop     | noop     | noop     | noop     | noop
         *   TASK_RUNNING_VALUE:  | started  | started  | started  | started  | started
         *   TASK_STAGING_VALUE:  | noop     | noop     | noop     | noop     | noop
         *   TASK_STARTING_VALUE: | starting | starting | starting | starting | starting
         *   (kill from user      | killed   | killed   | killed   | noop     | noop)
         *
         **/
        Stanchion.schedule(() -> {
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
        });
    }

    // Maybe Retry
    void retry(Protos.TaskStatus status) {
        String reason = "";
        if (status.hasMessage()) {
            reason = status.getMessage();
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

    private void updateOfferStats() {
        ResourceQuantity total = new ResourceQuantity();
        for (Map.Entry<String, Protos.Offer> e : OFFER_STOCK.entrySet()) {
            Resource r = ResourceConstructor.decode(e.getValue().getResourcesList());
            total.add(r.toQuantity());
        }
        total.setNodes(OFFER_STOCK.size());
        StatusCache.setOfferStats(OFFER_STOCK.size(), total);
    }

    // Get all running jobs and sync its latest state in Mesos
    // If it's not lost, just update state. Otherwise, set its state as QUEUED back.
    // This call must be offloaded from scheduler callback thread if schedule is active;
    // while if it's not active, it must block all other operations.
    private void maybeRecoverRunning(SchedulerDriver driver) {
        List<Job> jobs = Database.getInstance().getRunning();
        Database.getInstance().retryJobs(jobs.stream().map(job -> job.id()).collect(Collectors.toList()));
    }

    public boolean validateJob(Job job) {
        return MAX_JOB_SIZE.fits(job);
    }

    public ResourceQuantity maxJobSize() {
        return MAX_JOB_SIZE;
    }
}
