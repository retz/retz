package io.github.retz.scheduler;

import io.github.retz.web.WebConsole;
import io.github.retz.web.WebConsoleForReplica;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.cli.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;

import java.io.Closeable;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.Optional;


public class CuratorClient implements Closeable {
    
    private static final Logger LOG = LoggerFactory.getLogger(CuratorClient.class);
    private static CuratorFramework client = CuratorFrameworkFactory.builder().connectString("localhost:2180")
	.retryPolicy(new ExponentialBackoffRetry(3000, 5)).namespace("retz").build();;
    private static LeaderSelector leaderSelector = new LeaderSelector(client, "/master", new RetzLeaderSelectorListener());
    private static CountDownLatch leaderLatch = new CountDownLatch(1);
    private static CountDownLatch closeLatch = new CountDownLatch(1);
    private static MesosFrameworkLauncher.Configuration conf;
    private static int memberNum, quorum;    
    
    
    public static void startZk() {
	client.start();
    }

    public static void bootstrap() throws Exception {
	try {
	    byte[] quorumData = client.getData().forPath("/config/quorum");
	    byte[] memberNumData = client.getData().forPath("/config/memberNum");
	    quorum = ByteBuffer.wrap(quorumData).getInt();
	    memberNum = ByteBuffer.wrap(memberNumData).getInt();
	    LOG.info("quorum: {}, memberNum: {}", quorum, memberNum);
	} catch (Exception e) {
	    LOG.error(e.toString());       	   
	    byte[] quorumData = ByteBuffer.allocate(4).putInt(3).array();
	    byte[] memberNumData = ByteBuffer.allocate(4).putInt(5).array();
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

	// Start web server
	int port = conf.getPort();
	WebConsole webConsole = new WebConsole(port);
	WebConsole.setScheduler(scheduler);
	WebConsole.setDriver(driver);
	LOG.info("Web console has started with port {}", port);

	// Stop them all
	// Wait for Mesos framework stop
	status = driver.join();
	LOG.info("{} has been stopped: {}", RetzScheduler.FRAMEWORK_NAME, status.name());	

	webConsole.stop(); // Stop web server	
    }

    public static void runForReplica() {
	LOG.info("I am a replica");
	WebConsoleForReplica webConsoleForReplica = new WebConsoleForReplica(9091);
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
    
    public static int run(String... argv) { 
	try {
	    conf = MesosFrameworkLauncher.parseConfiguration(argv);
	    startZk();
	    bootstrap();
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
