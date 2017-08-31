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
package io.github.retz.protocol.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.github.retz.protocol.data.Job.JobState.*;

public class Job {
    private final String cmd;
    private final String appid;
    private final Set<String> tags;
    private final ResourceQuantity resources;
    private Optional<String> attributes;
    private String scheduled;
    private String started;
    private String finished;
    private Properties props;
    private int result = -1;
    private int id;
    private String url;
    private String reason;
    private int retry; // How many retry now we have
    private int priority;
    private String name; // TODO: make this configurable;
    private String taskId; // TaskId assigned by Mesos (or other scheduler)
    private String slaveId;
    private JobState state;

    public Job(String appName, String cmd, Properties props, int cpu, int memMB, int disk) {
        this.appid = appName;
        this.cmd = cmd;
        this.name = Integer.toString(cmd.hashCode());
        this.tags = new HashSet<>();
        this.props = props;
        assert cpu > 0 && memMB >= 32 && disk >= 0;
        this.resources = new ResourceQuantity(cpu, memMB, 0, 0, 0, 1);
        this.attributes = Optional.empty();
        this.state = CREATED;
        this.retry = 0;
        this.priority = 0;
        this.taskId = null;
        this.slaveId = null;
    }

    public Job(String appName, String cmd, Properties props, int cpu, int memMB, int disk, int gpu, int ports) {
        this(appName, cmd, props, cpu, memMB, disk);
        Objects.requireNonNull(gpu);
        resources.add(0, 0, gpu, ports, disk);
    }

    public Job(String appName, String cmd, Properties props, int cpu, int memMB, int disk, int gpu, int ports,
               String attributes) {
        this(appName, cmd, props, cpu, memMB, disk, gpu, ports);
        this.attributes = Optional.of(attributes);
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
               @JsonProperty("priority") int priority,
               @JsonProperty(value = "appid", required = true) String appid,
               @JsonProperty(value = "name") String name,
               @JsonProperty("tags") Set<String> tags,
               @JsonProperty(value = "resources", required = true) ResourceQuantity resources,
               @JsonProperty("attributes") Optional<String> attributes,
               @JsonProperty("taskId") String taskId,
               @JsonProperty("slaveId") String slaveId,
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
        this.priority = priority;
        this.appid = appid;
        this.name = name;
        this.tags = (tags == null) ? new HashSet<>() : tags;
        assert resources.getCpu() > 0;
        assert resources.getMemMB() >= 32;
        this.resources = resources;
        this.attributes = attributes;
        this.taskId = taskId;
        this.slaveId = slaveId;
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

    @JsonGetter("priority")
    public int priority() {
        return priority;
    }

    @JsonGetter("appid")
    public String appid() {
        return appid;
    }

    @JsonGetter("name")
    public String name() {
        return name;
    }

    @JsonGetter
    public Set<String> tags() {
        return tags;
    }

    @JsonGetter("resources")
    public ResourceQuantity resources() {
        return resources;
    }

    @JsonGetter("attributes")
    public Optional<String> attributes() {
        return attributes;
    }

    @JsonGetter("taskId")
    public String taskId() {
        return taskId;
    }

    @JsonGetter("slaveId")
    public String slaveId() {
        return slaveId;
    }

    @JsonGetter("state")
    public JobState state() {
        return state;
    }

    public void schedule(int id, String now) {
        this.id = id;
        this.scheduled = now;
        this.state = QUEUED;
    }

    public void doRetry() {
        this.state = QUEUED;
        this.retry++;
    }

    public void starting(String taskId, Optional<String> maybeUrl, String now) {
        this.started = now;
        this.taskId = taskId;
        if (maybeUrl.isPresent()) {
            this.url = maybeUrl.get();
        }
        this.state = STARTING;
    }

    public void started(String taskId, String slaveId, Optional<String> maybeUrl, String now) {
        this.started = now;
        this.taskId = taskId;
        this.slaveId = Objects.requireNonNull(slaveId);
        if (maybeUrl.isPresent()) {
            this.url = maybeUrl.get();
        }
        this.state = STARTED;
    }

    public void finished(String now, Optional<String> url, int result) {
        this.finished = now;
        this.result = result;
        if (url.isPresent()) {
            this.url = url.get();
        }
        this.state = FINISHED;
    }

    public void killed(String now, Optional<String> maybeUrl, String reason) {
        this.finished = now;
        this.reason = reason;
        if (maybeUrl.isPresent()) {
            this.url = maybeUrl.get();
        }
        this.state = KILLED;
    }

    public void setPriority(int p) {
        this.priority = p;
    }

    public void setName(String name) {
        if (name != null) {
            this.name = name;
        }
    }

    public void addTags(String... tags) {
        addTags(Arrays.asList(tags));
    }

    public void addTags(Collection<String> tags) {
        for (String tag : tags) {
            this.tags.add(tag);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{")
                .append("id=").append(id)
                .append(", name=").append(name)
                .append(", tags=").append(tags.stream().sorted().collect(Collectors.joining(",", "[", "]")))
                .append(", appid=").append(appid)
                .append(", cmd=").append(cmd)
                .append(", env=").append(maskProperties(props))
                .append(", resources=").append(resources.toString());

        sb.append(", priority=").append(priority);
        if (scheduled != null) {
            sb.append(", scheduled=").append(scheduled);
        }
        if (started != null) {
            sb.append(", started=").append(started);
        }
        if (finished != null) {
            sb.append(", finished=").append(finished);
        }
        sb.append(", state=").append(state);
        if (state == FINISHED) {
            sb.append(", result=").append(result);
        }
        if (result != 0) {
            sb.append(", reason=").append(reason);
        }
        sb.append(", taskid=").append(taskId)
                .append(", slaveid=").append(slaveId)
                .append("}");
        return sb.toString();
    }

    public String pp() {
        StringBuilder sb = new StringBuilder("{")
                .append("id=").append(id)
                .append(", name=").append(name)
                .append(", tags=").append(tags.stream().sorted().collect(Collectors.joining(",", "[", "]")))
                .append(", appid=").append(appid)
                .append(", cmd=").append(cmd)
                .append(", env=").append(maskProperties(props))
                .append(", resources=").append(resources.toString());

        sb.append(", priority=").append(priority);
        if (scheduled != null) {
            sb.append(", scheduled=").append(scheduled);
        }
        if (started != null) {
            sb.append(", started=").append(started);
        }
        if (finished != null) {
            sb.append(", finished=").append(finished);
        }
        sb.append(", state=").append(state);
        if (state == FINISHED) {
            sb.append(", result=").append(result);
        }
        if (reason != null) {
            sb.append(", reason=").append(reason);
        }
        if (taskId != null) {
            sb.append(", taskid=").append(taskId);
        }
        if (slaveId != null) {
            sb.append(", slaveid=").append(slaveId);
        }
        return sb.append("}").toString();
    }

    /**
     * State diagram:
     * [CREATED] ---&gt; [QUEUED] ---&gt; [STARTED] ---&gt; [FINISHED]
     * |              +--------&gt; [KILLED]
     * +----------------------------^
     */
    public enum JobState { // TODO: define correspondce against Mesos Task status
        CREATED,
        QUEUED,
        STARTING,
        STARTED,
        FINISHED,
        KILLED,
    }

    private Properties maskProperties(Properties props) {
        Pattern p = Pattern.compile(".*(secret|password|token).*", Pattern.CASE_INSENSITIVE);
        Properties maskedProps = new Properties();
        props.forEach((key, value) -> {
            String k = key.toString();
            String v = (p.matcher(k).matches()) ? "<masked>" : value.toString();
            maskedProps.setProperty(k, v);
        });
        return maskedProps;
    }

}
