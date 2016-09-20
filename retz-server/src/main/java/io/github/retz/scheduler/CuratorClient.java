package io.github.retz.scheduler;

import io.github.retz.web.WebConsole;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.cli.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
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


public class CuratorClient implements Closeable, LeaderSelectorListener {
    
    private static final Logger LOG = LoggerFactory.getLogger(CuratorClient.class);
    private static CuratorClient instance = null;
    private CuratorFramework client;
    private LeaderSelector leaderSelector;
    private static CountDownLatch leaderLatch = new CountDownLatch(1);
    private static CountDownLatch closeLatch = new CountDownLatch(1);
    private static MesosFrameworkLauncher.Configuration conf;
    private static int memberNum, quorum;    
    
    private CuratorClient() {
	this.client = CuratorFrameworkFactory.builder().connectString("localhost:2180")
	    .retryPolicy(new ExponentialBackoffRetry(3000, 5)).namespace("retz").build();
	this.leaderSelector = new LeaderSelector(this.client, "/master", this);
    }

    public static synchronized CuratorClient getInstance() {
	if (instance == null) {
	    instance = new CuratorClient();
	}
	return instance;
    }
    
    public void startZk() {
	this.client.start();
    }

    public void bootstrap() throws Exception {
	try {
	    byte[] quorumData = this.client.getData().forPath("/config/quorum");
	    byte[] memberNumData = this.client.getData().forPath("/config/memberNum");
	    quorum = ByteBuffer.wrap(quorumData).getInt();
	    memberNum = ByteBuffer.wrap(memberNumData).getInt();
	    LOG.info("quorum: {}, memberNum: {}", quorum, memberNum);
	} catch (Exception e) {
	    LOG.error(e.toString());       	   
	    byte[] quorumData = ByteBuffer.allocate(4).putInt(3).array();
	    byte[] memberNumData = ByteBuffer.allocate(4).putInt(5).array();
	    this.client.create().creatingParentsIfNeeded().forPath("/config/quorum", quorumData);
	    this.client.create().creatingParentsIfNeeded().forPath("/config/memberNum", memberNumData);	    
	}
    }
    
    public void startMasterSelection() throws InterruptedException, Exception {
	LOG.info("Starting master selection");	
	this.leaderSelector.start();
	if (!awaitLeadership()) {
	    LOG.info("awaitLeadership is timedout");
	}

	while (!leaderSelector.hasLeadership()) {
	    LOG.info("I am a Replica");
	    Thread.sleep(3000);
	}
    }
    
    public boolean awaitLeadership() throws InterruptedException {
	return leaderLatch.await(3000, TimeUnit.MILLISECONDS);
    }
    
    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
	
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
    
    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
	switch (newState) {
	case CONNECTED:
	    break;
	case RECONNECTED:
	    break;
	case SUSPENDED:
	    break;
	case LOST:
	    try {
		close();
	    } catch (IOException e) {
		LOG.error(e.toString());
	    }
	    break;
	case READ_ONLY:
	    break;
	} 
    }

    @Override
    public void close() throws IOException {
	LOG.info("Closing");
	closeLatch.countDown();
	this.leaderSelector.close();
	this.client.close();
    }

    public Optional<Protos.FrameworkID> getFrameworkId() {
	try {
	    LOG.info("Fetching the stored frameworkId");
	    byte[] frameworkIdData = this.client.getData().forPath("/id");
	    Protos.FrameworkID frameworkId = Protos.FrameworkID.newBuilder().setValue(new String(frameworkIdData)).build();
	    return Optional.of(frameworkId);
	} catch (Exception e) {
	    LOG.error(e.toString());
	    return Optional.empty();
	}
    }
    
    public void storeFrameworkId(Protos.FrameworkID frameworkId) {
	try {
	    this.client.create().forPath("/id", frameworkId.getValue().getBytes());
	} catch (Exception e) {
	    // Do nothing
	}
    }
    
    public static int run(String... argv) {	

	try {
	    conf = MesosFrameworkLauncher.parseConfiguration(argv);
	} catch (ParseException e) {
	    LOG.error(e.toString());
	    return -1;
	} catch (URISyntaxException e) {
	    LOG.error(e.toString());
	    return -1;
	} catch (IOException e) {
	    LOG.error(e.toString());
	    return -1;
	}	

	try {
	    CuratorClient curatorClient = CuratorClient.getInstance();
	    curatorClient.startZk();
	    curatorClient.bootstrap();
	    curatorClient.startMasterSelection();
	    closeLatch.await();
	    return 0;
	} catch (Exception e) {
	    LOG.error(e.toString());
	    return -1;
	}
    }
    
    public static void main(String... argv) {
	System.exit(run(argv));
    }
}
