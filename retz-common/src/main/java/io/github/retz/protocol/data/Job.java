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
package io.github.retz.protocol.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Properties;

import static io.github.retz.protocol.data.Job.JobState.*;

public class Job {
    private final String cmd;
    private String scheduled;
    private String started;
    private String finished;
    private Properties props;
    private int result = -1;
    // REVIEW: Use @JsonProperty(required=true) only if it is mandatory,
    private int id; // TODO This becomes 0 when the JSON property is absent...
    private String url;
    private String reason;
    private int retry; // How many retry now we have

    private final String appid;
    private String name;
    private final Range cpu;
    private final Range memMB;
    private Range gpu;

    /**
     * State diagram:
     *  [CREATED] ---&gt; [QUEUED] ---&gt; [STARTED] ---&gt; [FINISHED]
     *                    |              +--------&gt; [KILLED]
     *                    +----------------------------^
     */
    public enum JobState {
        CREATED,
        QUEUED,
        STARTED,
        FINISHED,
        KILLED,
    }

    private JobState state;

    private boolean trustPVFiles = false;

    public Job(String appName, String cmd, Properties props, Range cpu, Range memMB) {
        this.appid = appName;
        this.cmd = cmd;
        this.props = props;
        assert (cpu.getMin() > 0 && memMB.getMin() >= 32);
        this.cpu = cpu;
        this.memMB = memMB;
        this.gpu = new Range(0, 0);
        this.state = CREATED;
        this.retry = 0;
    }

    public Job(String appName, String cmd, Properties props, Range cpu, Range memMB, Range gpu) {
        this(appName, cmd, props, cpu, memMB);
        if (gpu != null ) {
            this.gpu = gpu;
        }
    }

    @JsonCreator
    public Job(@JsonProperty(value = "cmd", required = true) String cmd,
               @JsonProperty("scheduled") String scheduled,
               @JsonProperty("started") String started,
               @JsonProperty("finished") String finished,
               @JsonProperty("env") Properties props,
               @JsonProperty("result") int result,
               @JsonProperty(value = "id", required = true) int id,
               @JsonProperty("url") String url,
               @JsonProperty("reason") String reason,
               @JsonProperty("retry") int retry,
               @JsonProperty(value = "appid", required = true) String appid,
               @JsonProperty(value = "name") String name,
               @JsonProperty("cpu") Range cpu,
               @JsonProperty("memMB") Range memMB,
               @JsonProperty("gpu") Range gpu,
               @JsonProperty("trustPVFiles") boolean trustPVFiles,
               @JsonProperty("state") JobState state) {
        this.cmd = Objects.requireNonNull(cmd);
        this.scheduled = scheduled;
        this.started = started;
        this.finished = finished;
        this.props = props;
        this.result = result;
        this.id = Objects.requireNonNull(id);
        this.url = url;
        this.reason = reason;
        this.retry = retry;
        this.appid = appid;
        this.name = (name != null) ? name : appid;
        this.cpu = cpu;
        this.memMB = memMB;
        this.gpu = gpu;
        this.trustPVFiles = trustPVFiles;
        this.state = Objects.requireNonNull(state);
    }

    @JsonGetter("cmd")
    public String cmd() {
        return cmd;
    }

    @JsonGetter("scheduled")
    public String scheduled() {
        return scheduled;
    }

    @JsonGetter("started")
    public String started() {
        return started;
    }

    @JsonGetter("props")
    public Properties props() {
        return props;
    }

    @JsonGetter("finished")
    public String finished() {
        return finished;
    }

    @JsonGetter("result")
    public int result() {
        return result;
    }

    @JsonGetter("id")
    public int id() {
        return id;
    }

    @JsonGetter("url")
    public String url() {
        return url;
    }

    @JsonGetter("reason")
    public String reason() {
        return reason;
    }

    @JsonGetter("retry")
    public int retry() {
        return retry;
    }

    @JsonGetter("appid")
    public String appid() {
        return appid;
    }

    @JsonGetter("name")
    public String name() {
        return name;
    }

    @JsonGetter("cpu")
    public Range cpu() {
        return cpu;
    }

    @JsonGetter("memMB")
    public Range memMB() {
        return memMB;
    }

    @JsonGetter("gpu")
    public Range gpu() {
        return gpu;
    }

    @JsonGetter("trustPVFiles")
    public boolean trustPVFiles() {
        return trustPVFiles;
    }

    @JsonGetter("state")
    public JobState state() {
        return state;
    }

    public void setTrustPVFiles(boolean trustPVFiles) {
        this.trustPVFiles = trustPVFiles;
    }

    public void schedule(int id, String now) {
        this.id = id;
        this.scheduled = now;
        this.state = QUEUED;
    }

    public void setStarted(String now) {
        this.started = now;
        this.state = STARTED;
    }

    public void doRetry() {
        this.retry++;
    }

    public void finished(String url, String now, int result) {
        this.url = url;
        this.finished = now;
        this.result = result;
        this.state = FINISHED;
    }

    public void killed(String now, String reason) {
        this.finished = now;
        this.reason = reason;
        this.state = KILLED;
    }

}
