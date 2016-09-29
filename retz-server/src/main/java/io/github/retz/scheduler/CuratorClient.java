package io.github.retz.scheduler;

import io.github.retz.protocol.*;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.web.WebConsole;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.cli.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.zookeeper.CreateMode;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class CuratorClient implements Closeable {
    
    private static final Logger LOG = LoggerFactory.getLogger(CuratorClient.class);
    private static CuratorFramework client = CuratorFrameworkFactory.builder().connectString("localhost:2180")
	.retryPolicy(new ExponentialBackoffRetry(3000, 5)).namespace("retz").build();;
    private static LeaderSelector leaderSelector = new LeaderSelector(client, "/master", new RetzLeaderSelectorListener());
    private static CountDownLatch leaderLatch = new CountDownLatch(1);
    private static CountDownLatch closeLatch = new CountDownLatch(1);
    private static MesosFrameworkLauncher.Configuration conf;
    private static int memberNum, quorum;  
    private static List<io.github.retz.web.master.Client> webClientsForMaster =
	new ArrayList<io.github.retz.web.master.Client>();
    
    
    public static void bootstrap() throws Exception {
        try {
            byte[] quorumData = client.getData().forPath("/config/quorum");
            byte[] memberNumData = client.getData().forPath("/config/memberNum");
            quorum = ByteBuffer.wrap(quorumData).getInt();
            memberNum = ByteBuffer.wrap(memberNumData).getInt();
            LOG.info("quorum: {}, memberNum: {}", quorum, memberNum);	 
        } catch (Exception e) {
            LOG.error(e.toString());
            quorum = 2;
            memberNum = 3;
            byte[] quorumData = ByteBuffer.allocate(4).putInt(quorum).array();
            byte[] memberNumData = ByteBuffer.allocate(4).putInt(memberNum).array();
            client.create().creatingParentsIfNeeded().forPath("/config/quorum", quorumData);
            client.create().creatingParentsIfNeeded().forPath("/config/memberNum", memberNumData);	    
        }
    }
    
    public static void startMasterSelection() throws InterruptedException, Exception {
        LOG.info("Starting master selection");	       
        leaderSelector.start();
        if (!leaderLatch.await(3000, TimeUnit.MILLISECONDS)) {
            LOG.info("awaitLeadership is timedout");
        }      	
        if (!leaderSelector.hasLeadership()) {
            runForReplica();
        }
    }
    
    public static void runForMaster() throws Exception {
        
        LOG.info("Leadership taken");
        leaderLatch.countDown();
    	JobQueue.print();
        try {
            client.delete().forPath("/members/replicas/localhost:" + conf.getPort()); 
        } catch (Exception e) {
        }
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/members/master/localhost:" + conf.getPort());
        
        while (client.getChildren().forPath("/members/replicas").size() + 1 < quorum) {
            Thread.sleep(3000);
        }
        LOG.info("{} retz-server is running", quorum);
    	
        for (String replica : client.getChildren().forPath("/members/replicas")) {
            LOG.info("replica: {}", replica);
            URI uri = new URI("http://" + replica);
            webClientsForMaster.add(new io.github.retz.web.master.Client(uri.getHost(), uri.getPort()));
        }
    	
        Protos.FrameworkInfo fw = MesosFrameworkLauncher.buildFrameworkInfo(conf);
    	
        RetzScheduler scheduler = new RetzScheduler(conf, fw);
        MesosSchedulerDriver driver = null;
        try {
            driver = new MesosSchedulerDriver(scheduler, fw, conf.getMesosMaster());
        } catch (Exception e) {
            LOG.error("Cannot start Mesos scheduler: {}", e.toString());
            System.exit(-1);
    	}
    	
        Protos.Status status = driver.start();
    
        if (status != Protos.Status.DRIVER_RUNNING) {
            LOG.error("Cannot start Mesos scheduler: {}", status.name());
            System.exit(-1);
        }
    	
        LOG.info("Mesos scheduler started: {}", status.name());

        // Set scheduler and driver
        io.github.retz.web.WebConsole.setScheduler(scheduler);
        io.github.retz.web.WebConsole.setDriver(driver);
    	
    	// Stop them all
    	// Wait for Mesos framework stop
        status = driver.join();
        LOG.info("{} has been stopped: {}", RetzScheduler.FRAMEWORK_NAME, status.name());	    
    }

    public static void runForReplica() {
        LOG.info("I am a replica");
        try {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/members/replicas/localhost:" + conf.getPort());
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }
    
    @Override
    public void close() throws IOException {
        LOG.info("Closing");
        closeLatch.countDown();
        leaderSelector.close();
        client.close();
    }

    public static Optional<Protos.FrameworkID> getFrameworkId() {
        try {
            LOG.info("Fetching the stored frameworkId");
            byte[] frameworkIdData = client.getData().forPath("/id");
            Protos.FrameworkID frameworkId = Protos.FrameworkID.newBuilder().setValue(new String(frameworkIdData)).build();
            return Optional.of(frameworkId);
        } catch (Exception e) {
            LOG.error(e.toString());
            return Optional.empty();
        }
    }
    
    public static void storeFrameworkId(Protos.FrameworkID frameworkId) {
        try {
            client.create().forPath("/id", frameworkId.getValue().getBytes());
        } catch (Exception e) {
            // Do nothing
        }
    }
    
    public static Optional<URI> getMasterUri() {
        try {
            List<String> master = client.getChildren().forPath("/members/master");
            assert master.size() == 1;
            return Optional.of(new URI("http://" + master.get(0)));
        } catch (Exception e) {
            LOG.error(e.toString());
        }
        return Optional.empty();
    }
    
    public static void sendLoadAppFromMasterRequest(Application app) {
        if (!leaderSelector.hasLeadership()) {
            return;
        }
        for (io.github.retz.web.master.Client c : webClientsForMaster) {
            try {
                Response res = c.loadAppFromMaster(app);
                LOG.info(res.status());              
            } catch (IOException e) {
                LOG.error(e.toString());
            }
        }
    }
    
    public static void sendIncCounterRequest() {
        if (!leaderSelector.hasLeadership()) {
            return;
        }
        for (io.github.retz.web.master.Client c : webClientsForMaster) {
            try {
                Response res = c.incCounter();
                LOG.info(res.status());              
            } catch (IOException e) {
                LOG.error(e.toString());
            }
        }
    }
    
    public static void sendPushJobQueueRequest(Job job) {
        if (!leaderSelector.hasLeadership()) {
            return;
        }
        for (io.github.retz.web.master.Client c : webClientsForMaster) {
            try {
                Response res = c.pushJobQueue(job);
                LOG.info(res.status());              
            } catch (IOException e) {
                LOG.error(e.toString());
            }
        }
    }
    
    public static void sendPopJobQueueRequest() {
        if (!leaderSelector.hasLeadership()) {
            return;
        }
        for (io.github.retz.web.master.Client c : webClientsForMaster) {
            try {
                Response res = c.popJobQueue();
                LOG.info(res.status());              
            } catch (IOException e) {
                LOG.error(e.toString());
            }
        }
    }
    
    public static void sendPutRunningRequest(String taskId, Job job) {
        if (!leaderSelector.hasLeadership()) {
            return;
        }
        for (io.github.retz.web.master.Client c : webClientsForMaster) {
            try {
                Response res = c.putRunning(taskId, job);
                LOG.info(res.status());              
            } catch (IOException e) {
                LOG.error(e.toString());
            }
        }
    }
    
    public static void sendRemoveRunningRequest(String taskId) {
        if (!leaderSelector.hasLeadership()) {
            return;
        }
        for (io.github.retz.web.master.Client c : webClientsForMaster) {
            try {
                Response res = c.removeRunning(taskId);
                LOG.info(res.status());              
            } catch (IOException e) {
                LOG.error(e.toString());
            }
        }
    }
    
    public static void sendAddFinishedRequest(Job job) {
        if (!leaderSelector.hasLeadership()) {
            return;
        }
        for (io.github.retz.web.master.Client c : webClientsForMaster) {
            try {
                Response res = c.addFinished(job);
                LOG.info(res.status());              
            } catch (IOException e) {
                LOG.error(e.toString());
            }
        }
    }
    
    public static int run(String... argv) { 
        try {
            conf = MesosFrameworkLauncher.parseConfiguration(argv);
            client.start();
            bootstrap();
            // Start web server
            int port = conf.getPort();
            WebConsole webConsole = new WebConsole(port);
            LOG.info("Web console has started with port {}", port);
            startMasterSelection();
            closeLatch.await();
            return 0;	    
        } catch (ParseException e) {
            LOG.error(e.toString());
            return -1;
        } catch (URISyntaxException e) {
            LOG.error(e.toString());
            return -1;
        } catch (IOException e) {
            LOG.error(e.toString());
            return -1;
        } catch (Exception e) {
            LOG.error(e.toString());
            return -1;
        }
    }
    
    public static void main(String... argv) {
        System.exit(run(argv));
    }
}
