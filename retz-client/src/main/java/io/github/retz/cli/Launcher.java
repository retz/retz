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
package io.github.retz.cli;

import io.github.retz.protocol.*;
import io.github.retz.web.Client;
import org.apache.commons.cli.*;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.*;
import java.util.*;

import static java.util.Arrays.asList;

public class Launcher {

    static final Logger LOG = LoggerFactory.getLogger(Launcher.class);
    static final Option OPT_CONFIG;
    static final Option OPT_YAESS_COMMAND; // YAESS command to be run at remote node
    static final Option OPT_APP_NAME; // Application name to be loaded
    static final Option OPT_PERSISTENT_FILE_URIS;
    static final Option OPT_FILE_URIS; // Application files
    //static final Option OPT_JOB_NAME;
    //static final Option OPT_JOB_LIST_FILE;
    //static final Option OPT_JOB_RETRY;
    static final Option OPT_JOB_ENV;
    static final Option OPT_JOB_RESULTS; // directory to save job results in local
    static final Option OPT_CPU;
    static final Option OPT_MEM_MB;
    static final Option OPT_DISK_MB;
    static final Option OPT_GPU;
    static final Option OPT_TRUST_PVFILES;
    static final Option OPT_JOB_ID; // only used in get-job request

    private static final Options OPTIONS;

    static {
        OPT_CONFIG = new Option("C", "config", true, "Configuration file path");
        OPT_CONFIG.setArgName("/opt/retz-client/etc/retz.properties");

        OPT_YAESS_COMMAND = new Option("cmd", true, "Remote YAESS invocation command");
        OPT_YAESS_COMMAND.setArgName("yaess/bin/yaess-batch.sh example.foobar [ARGS]");

        OPT_APP_NAME = new Option("A", "appname", true, "Asakusa Application name");
        OPT_APP_NAME.setArgName("your-app-name");

        OPT_PERSISTENT_FILE_URIS = new Option("P", "persistent", true, "Asakusa Application environment persistent files");
        OPT_PERSISTENT_FILE_URIS.setArgName("http://server:8000/path/data.tar.gz,https://server:8000/file2.tar.gz");

        OPT_FILE_URIS = new Option("F", "file", true, "Asakusa Application environment files");
        OPT_FILE_URIS.setArgName("http://server:8000/path/data.tar.gz,https://server:8000/file2.tar.gz");

        OPT_JOB_ENV = new Option("E", "env", true, "Pairs of environment variable names and values");
        OPT_JOB_ENV.setArgName("-E ASAKUSA_M3BP_OPTS='-Xmx32g' -E SPARK_CMD=path/to/spark-cmd");
        OPT_JOB_ENV.setValueSeparator('=');
        OPT_JOB_ENV.setArgs(2);

        OPT_JOB_RESULTS = new Option("R", "resultdir", true, "Directory to save job results");
        OPT_JOB_RESULTS.setArgName("/tmp/path/to/dir");

        OPT_CPU = new Option("cpu", true, "Range of CPU cores assigned to the job");
        OPT_CPU.setArgName("2-");
        OPT_MEM_MB = new Option("mem", true, "Range of size of RAM(MB) assigned to the job");
        OPT_MEM_MB.setArgName("512-");
        OPT_DISK_MB = new Option("disk", true, "Disk size for persistent volume in MB");
        OPT_DISK_MB.setArgName("1024");
        OPT_GPU = new Option("gpu", true, "Range of GPU cards assigned to the job");
        OPT_GPU.setArgName("1-1");

        OPT_TRUST_PVFILES = new Option("trustpvfiles", false, "Whether to trust decompressed files in persistent volume from -P option");

        OPT_JOB_ID = new Option("id", true, "Job ID whose state and details you want");
        OPT_JOB_ID.setArgName("234");

        OPTIONS = new Options();
        OPTIONS.addOption(OPT_CONFIG);
        OPTIONS.addOption(OPT_YAESS_COMMAND);
        OPTIONS.addOption(OPT_APP_NAME);
        OPTIONS.addOption(OPT_PERSISTENT_FILE_URIS);
        OPTIONS.addOption(OPT_FILE_URIS);
        OPTIONS.addOption(OPT_JOB_ENV);
        OPTIONS.addOption(OPT_JOB_RESULTS);
        OPTIONS.addOption(OPT_CPU);
        OPTIONS.addOption(OPT_MEM_MB);
        OPTIONS.addOption(OPT_DISK_MB);
        OPTIONS.addOption(OPT_GPU);
        OPTIONS.addOption(OPT_TRUST_PVFILES);
        OPTIONS.addOption(OPT_JOB_ID);
    }

    public static void main(String... argv) {
        System.setProperty("org.slf4j.simpleLogger.log.defaultLogLevel", "DEBUG");

        if (argv.length < 1) {
            help();
            System.exit(-1);
        }

        int status = execute(argv);
        System.exit(status);
    }

    public static int execute(String... argv) {
        Configuration conf;

        try {
            conf = parseConfiguration(argv);

            if (argv[0].equals("config")) {
                LOG.info("Configurations:");
                conf.printFileProperties();
                System.exit(0);


            } else if (oneOf(argv[0], "list", "schedule", "get-job", "run", "watch", "load-app", "list-app", "unload-app")) {
                String uri = new StringBuilder()
                        .append("ws://")
                        .append(conf.getUri().getHost())
                        .append(":")
                        .append(conf.getUri().getPort())
                        .append("/cui")
                        .toString();
                Client webClient = new Client(uri);
                try {
                    webClient.connect();
                    return doRequest(webClient, argv[0], conf);

                } catch (Exception e) {
                    e.printStackTrace();
                    return -1;
                } finally {
                    webClient.disconnect();
                }
  /*
                $ retz kill <jobid>
                $ retz schedule -file <list of batches in a text file>
*/

            }
            LOG.info(conf.toString());
            help();
        } catch (ParseException e) {
            LOG.error("ParseException: {}", e);
            help();
        } catch (IOException e) {
            LOG.error("file not found");
        } catch (IllegalArgumentException e) {
            LOG.error("Illegal Option specified: {}", e.getMessage());
            e.printStackTrace();
        } catch (URISyntaxException e) {
            LOG.error("Bad file format");
        }
        return -1;
    }

    private static boolean oneOf(String key, String... list) {
        for (String s : list) {
            if (key.equals(s)) return true;
        }
        return false;
    }

    private static int doRequest(Client c, String cmd, Configuration conf) throws IOException, InterruptedException {
        if (cmd.equals("list")) {
            ListJobResponse r = (ListJobResponse) c.list(64); // TODO: make this CLI argument
            List<Job> jobs = new LinkedList<>();
            jobs.addAll(r.queue());
            jobs.addAll(r.running());
            jobs.addAll(r.finished());

            TableFormatter formatter = new TableFormatter(
                    "TaskId", "State", "Result", "Duration", "AppName", "Command",
                    "Scheduled", "Started", "Finished", "Reason");

            jobs.sort((a, b) -> a.id() - b.id());

            for (Job job : jobs) {
                String state = "Queued";
                if (job.finished() != null) {
                    state = "Finished";
                } else if (job.started() != null) {
                    state = "Started";
                }
                String reason = "-";
                if (job.reason() != null) {
                    reason = "'" + job.reason() + "'";
                }
                String duration = "-";
                if (job.started() != null && job.finished() != null) {
                    try {
                        duration = Double.toString(TimestampHelper.diffMillisec(job.finished(), job.started()) / 1000.0);
                    }catch (java.text.ParseException e) {
                    }
                }
                formatter.feed(Integer.toString(job.id()), state, Integer.toString(job.result()), duration, job.appid(), job.cmd(),
                        job.scheduled(), job.started(), job.finished(), reason);
            }
            LOG.info(formatter.titles());
            for (String line : formatter) {
                LOG.info(line);
            }
            return 0;

        } else if (cmd.equals("schedule")) {
            if (!conf.remoteCommand.isPresent()) { // || !conf.applicationTarball.isPresent()) {
                LOG.error("No command specified");
                return -1;
            }
            try {
                Job job = new Job(conf.getAppName().get(), conf.getRemoteCommand().get(),
                        conf.getJobEnv(), conf.getCpu(), conf.getMemMB(), conf.getGPU());
                job.setTrustPVFiles(conf.getTrustPVFiles());
                LOG.info("Sending job {} to App {}", job.cmd(), job.appid());
                Response res = c.schedule(job);
                if (res instanceof ScheduleResponse) {
                    ScheduleResponse res1 = (ScheduleResponse) res;
                    LOG.info("Job (id={}): {} registered at {}", res1.job().id(), res1.status(), res1.job.scheduled());
                    return 0;
                } else {
                    LOG.error("Error: " + res.status());
                    return -1;
                }
            } catch (IOException e) {
                LOG.error(e.getMessage());

            } catch (InterruptedException e) {
                LOG.error(e.getMessage());

            }

        } else if (cmd.equals("get-job")) {
            if (conf.getJobId().isPresent()) {
                Response res = c.getJob(conf.getJobId().get());
                if (res instanceof GetJobResponse) {
                    GetJobResponse getJobResponse = (GetJobResponse) res;

                    if (getJobResponse.job().isPresent()) {
                        Job job = getJobResponse.job().get();

                        LOG.info("Job: appid={}, id={}, scheduled={}, cmd='{}'", job.appid(), job.id(), job.scheduled(), job.cmd());
                        LOG.info("\tstarted={}, finished={}, result={}", job.started(), job.finished(), job.result());

                        if (conf.getJobResultDir().isPresent()) {
                            if (conf.getJobResultDir().get().equals("-")) {
                                LOG.info("==== Printing stdout of remote executor ====");
                                Client.catHTTPFile(job.url(), "stdout");
                                LOG.info("==== Printing stderr of remote executor ====");
                                Client.catHTTPFile(job.url(), "stderr");
                                LOG.info("==== Printing stdout-{} of remote executor ====", job.id());
                                Client.catHTTPFile(job.url(), "stdout-" + job.id());
                                LOG.info("==== Printing stderr-{} of remote executor ====", job.id());
                                Client.catHTTPFile(job.url(), "stderr-" + job.id());
                            } else {
                                Client.fetchHTTPFile(job.url(), "stdout", conf.getJobResultDir().get());
                                Client.fetchHTTPFile(job.url(), "stderr", conf.getJobResultDir().get());
                                Client.fetchHTTPFile(job.url(), "stdout-" + job.id(), conf.getJobResultDir().get());
                                Client.fetchHTTPFile(job.url(), "stderr-" + job.id(), conf.getJobResultDir().get());
                            }
                        }
                        return 0;

                    } else {
                        LOG.error("No such job: id={}", conf.getJobId());
                    }
                } else {
                    ErrorResponse errorResponse = (ErrorResponse) res;
                    LOG.error("Error: {}", errorResponse.status());
                }
            } else {
                LOG.error("get-job requires job id you want: {} specified", conf.getJobId());
            }

        } else if (cmd.equals("run")) {
            if (!conf.remoteCommand.isPresent()) { // || !conf.applicationTarball.isPresent()) {
                LOG.error("No command specified");
                return -1;
            }
            Job job = new Job(conf.getAppName().get(), conf.getRemoteCommand().get(),
                    conf.getJobEnv(), conf.getCpu(), conf.getMemMB(), conf.getGPU());
            job.setTrustPVFiles(conf.getTrustPVFiles());
            LOG.info("Sending job {} to App {}", job.cmd(), job.appid());
            Job result = c.run(job);

            if (result != null) {
                LOG.info("Job result files URL: {}", result.url());

                if (conf.getJobResultDir().isPresent()) {
                    if (conf.getJobResultDir().get().equals("-")) {
                        LOG.info("==== Printing stdout of remote executor ====");
                        Client.catHTTPFile(result.url(), "stdout");
                        LOG.info("==== Printing stderr of remote executor ====");
                        Client.catHTTPFile(result.url(), "stderr");
                        LOG.info("==== Printing stdout-{} of remote executor ====", result.id());
                        Client.catHTTPFile(result.url(), "stdout-" + result.id());
                        LOG.info("==== Printing stderr-{} of remote executor ====", result.id());
                        Client.catHTTPFile(result.url(), "stderr-" + result.id());
                    } else {
                        Client.fetchHTTPFile(result.url(), "stdout", conf.getJobResultDir().get());
                        Client.fetchHTTPFile(result.url(), "stderr", conf.getJobResultDir().get());
                        Client.fetchHTTPFile(result.url(), "stdout-" + result.id(), conf.getJobResultDir().get());
                        Client.fetchHTTPFile(result.url(), "stderr-" + result.id(), conf.getJobResultDir().get());
                    }
                }
                return result.result();
            }


        } else if (cmd.equals("watch")) {
            c.startWatch((watchResponse -> {
                StringBuilder b = new StringBuilder()
                        .append("event: ").append(watchResponse.event());

                if (watchResponse.job() != null) {
                    b.append(" Job ").append(watchResponse.job().id())
                            .append(" (app=").append(watchResponse.job().appid())
                            .append(") has ").append(watchResponse.event())
                            .append(" at ");
                    if (watchResponse.event().equals("started")) {
                        b.append(watchResponse.job().started());
                        b.append(" cmd=").append(watchResponse.job().cmd());

                    } else if (watchResponse.event().equals("scheduled")) {
                        b.append(watchResponse.job().scheduled());
                        b.append(" cmd=").append(watchResponse.job().cmd());

                    } else if (watchResponse.event().equals("finished")) {
                        b.append(watchResponse.job().finished());
                        b.append(" result=").append(watchResponse.job().result());
                        b.append(" url=").append(watchResponse.job().url());
                    } else {
                        b.append("unknown event(error)");
                    }
                }
                LOG.info(b.toString());
                return true;
            }));
            return 0;

        } else if (cmd.equals("load-app")) {
            if (conf.getAppName().isPresent()) {
                if (conf.getFileUris().isEmpty() || conf.getPersistentFileUris().isEmpty()) {
                    LOG.warn("No files specified; mesos-execute would rather suite your use case.");
                }
                if (!conf.getPersistentFileUris().isEmpty() && !conf.getDiskMB().isPresent()) {
                    LOG.error("Option '-disk' required when persistent files specified");
                    return -1;
                }

                LoadAppResponse r = (LoadAppResponse) c.load(conf.getAppName().get(),
                        conf.getPersistentFileUris(), conf.getFileUris(), conf.getDiskMB());
                LOG.info(r.status());
                return 0;
            }
            LOG.error("AppName is required for load-app");

        } else if (cmd.equals("list-app")) {
            ListAppResponse r = (ListAppResponse) c.listApp();
            for (Application a : r.applicationList()) {
                LOG.info("Application {}: fetch: {} persistent ({} MB): {}", a.getAppid(),
                        String.join(" ", a.getFiles()), a.getDiskMB(),
                        String.join(" ", a.getPersistentFiles()));
            }
            return 0;

        } else if (cmd.equals("unload-app")) {
            if (conf.getAppName().isPresent()) {
                UnloadAppResponse res = (UnloadAppResponse) c.unload(conf.getAppName().get());
                if (res.status().equals("ok")) {
                    LOG.info("Unload: {}", res.status());
                    return 0;
                }
            }
            LOG.error("unload-app requires AppName");

        }
        return -1;
    }

    private static void help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(Integer.MAX_VALUE);
        formatter.printHelp(
                MessageFormat.format(
                        "java -classpath ... {0}",
                        Launcher.class.getName()),

                OPTIONS,
                true);
    }

    static Configuration parseConfiguration(String[] args) throws ParseException, IOException, URISyntaxException {
        assert args != null;
        Configuration result = new Configuration();

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(OPTIONS, args);

        result.configFile = cmd.getOptionValue(OPT_CONFIG.getOpt(), "/opt/retz-client/etc/retz.properties");
        LOG.info("Configuration file: {}", result.configFile);
        result.fileConfig = new FileConfiguration(result.configFile);

        result.remoteCommand = Optional.ofNullable(cmd.getOptionValue(OPT_YAESS_COMMAND.getOpt()));
        // TODO: validation to prevent command injections
        LOG.info("Remote command: `{}`", result.remoteCommand);

        result.appName = Optional.ofNullable(cmd.getOptionValue(OPT_APP_NAME.getOpt()));
        LOG.info("Application: {}", result.appName);

        String persistentFiles = cmd.getOptionValue(OPT_PERSISTENT_FILE_URIS.getOpt());
        if (persistentFiles == null) {
            result.persistentFileUris = new LinkedList<>();
        } else {
            result.persistentFileUris = Arrays.asList(persistentFiles.split(","));
        }
        LOG.info("Persistent file locations: {}", result.persistentFileUris);

        String files = cmd.getOptionValue(OPT_FILE_URIS.getOpt());
        if (files == null) {
            result.fileUris = new LinkedList<>(); // just an empty list
        } else {
            result.fileUris = asList(files.split(","));
        }
        LOG.info("File locations: {}", result.fileUris);

        result.jobEnv = cmd.getOptionProperties(OPT_JOB_ENV.getOpt());
        LOG.info("Environment variable of job ... {}", result.jobEnv);

        result.jobResultDir = Optional.ofNullable(cmd.getOptionValue(OPT_JOB_RESULTS.getOpt()));
        LOG.info("Job results are to be saved at {}", result.jobResultDir);

        // TODO: move default value to somewhere else as static value
        // TODO: write tests on Range.parseRange and Range#toString
        result.cpu = Range.parseRange(cmd.getOptionValue(OPT_CPU.getOpt(), "2-"));
        result.memMB = Range.parseRange(cmd.getOptionValue(OPT_MEM_MB.getOpt(), "512-"));
        result.gpu = Range.parseRange(cmd.getOptionValue(OPT_GPU.getOpt(), "0-0"));
        LOG.info("Range of CPU and Memory: {} {}MB ({} gpus)", result.cpu, result.memMB, result.gpu);

        String maybeDiskMBstring = cmd.getOptionValue(OPT_DISK_MB.getOpt());
        if (maybeDiskMBstring == null) {
            result.diskMB = Optional.empty();
        } else {
            result.diskMB = Optional.ofNullable(Integer.parseInt(maybeDiskMBstring));
            LOG.info("Disk reservation size: {}MB", result.diskMB.get());
        }

        result.trustPVFiles = cmd.hasOption(OPT_TRUST_PVFILES.getOpt());
        LOG.info("Check PV files: {}", result.trustPVFiles);

        String jobIdStr = cmd.getOptionValue(OPT_JOB_ID.getOpt());
        if (jobIdStr == null) {
            result.jobId = Optional.empty();
        } else {
            result.jobId = Optional.of(Integer.parseInt(jobIdStr));
        }

        return result;
    }

    private static Map<String, String> toMap(Properties p) {
        assert p != null;
        Map<String, String> results = new TreeMap<>();
        for (Map.Entry<Object, Object> entry : p.entrySet()) {
            results.put((String) entry.getKey(), (String) entry.getValue());
        }
        return results;
    }

    public static final class Configuration {
        String configFile;
        Properties fileProperties = new Properties();
        Optional<String> remoteCommand;
        Optional<String> appName;
        List<String> fileUris;
        List<String> persistentFileUris;

        Properties jobEnv;
        Optional<String> jobResultDir;
        Range cpu;
        Range memMB;
        Range gpu;
        Optional<Integer> diskMB;
        boolean trustPVFiles;

        Optional<Integer> jobId;

        FileConfiguration fileConfig;

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("[")
                    .append("configFile=").append(configFile)
                    .append(", fileProperties=").append(fileProperties)
                    .append(", remoteCommand=").append(remoteCommand)
                    .append(", appName=").append(appName)
                    .append(", fileUris=[").append(String.join(", ", fileUris)).append("]")
                    .append(", persistentFileUris=]").append(String.join(", ", persistentFileUris)).append("]")
                    .append(", jobEnv=").append(jobEnv)
                    .append(", jobResultDir=").append(jobResultDir)
                    .append(", cpu=").append(cpu)
                    .append(", mem=").append(memMB)
                    .append(", disk=").append(diskMB)
                    .append(", gpu=").append(gpu)
                    .append(", fileConfig=").append(fileConfig)
                    .append(", trustPVFiles=").append(trustPVFiles)
                    .append(", jobId=").append(jobId)
                    .append("]");
            return builder.toString();
        }

        public Optional<String> getAppName() {
            return appName;
        }

        public List<String> getPersistentFileUris() {
            return persistentFileUris;
        }

        public List<String> getFileUris() {
            return fileUris;
        }

        public Optional<String> getRemoteCommand() {
            return remoteCommand;
        }

        public Properties getJobEnv() {
            return jobEnv;
        }

        public Optional<String> getJobResultDir() {
            return jobResultDir;
        }

        public Range getCpu() {
            return cpu;
        }

        public Range getMemMB() {
            return memMB;
        }

        public Optional<Integer> getDiskMB() {
            return diskMB;
        }

        public Range getGPU() {
            return gpu;
        }

        public boolean getTrustPVFiles() {
            return trustPVFiles;
        }

        public Optional<Integer> getJobId() {
            return jobId;
        }

        public URI getUri() {
            return fileConfig.getUri();
        }

        public void printFileProperties() {
            for (Map.Entry property : fileProperties.entrySet()) {
                LOG.info("{}\t= {}", property.getKey().toString(), property.getValue().toString());
            }
        }
    }


}

