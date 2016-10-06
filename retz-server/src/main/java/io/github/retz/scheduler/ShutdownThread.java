package io.github.retz.scheduler;

import io.github.retz.web.WebConsole;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShutdownThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(ShutdownThread.class);

    private WebConsole console;
    private SchedulerDriver driver;

    public ShutdownThread(WebConsole console, SchedulerDriver driver) {
        this.console = console;
        this.driver = driver;
    }
    public void run() {
        LOG.info("Retz shutting down");
        // TODO: graceful stop
        // Close up all incoming requests to prevent database update
        console.stop();
        // Close all database connections; it may take time
        Database.stop();
        // Shut down connection to Mesos
        driver.stop();
        LOG.info("All clean up finished");
    }
}
