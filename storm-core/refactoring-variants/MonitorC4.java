/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.utils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GetInfoOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.NumErrorsChoice;
import org.apache.storm.generated.TopologyInfo;

public class MonitorC4 {

    private static final String WATCH_TRANSFERRED = "transferred";
    private static final String WATCH_EMITTED = "emitted";
    private static final String ALL_TIME_WINDOW = ":all-time";

    private int interval = 4;
    private String topology;
    private String component;
    private String stream;
    private String watch;

    private Set<String> getComponents(Nimbus.Iface client, String topologyName)
            throws Exception {

        Set<String> components = new HashSet<>();
        TopologyInfo info = getTopologyInfo(client, topologyName);

        for (ExecutorSummary executor : info.get_executors()) {
            components.add(executor.get_component_id());
        }

        return components;
    }

    private TopologyInfo getTopologyInfo(
            Nimbus.Iface client,
            String topologyName
    ) throws Exception {

        GetInfoOptions getInfoOptions = new GetInfoOptions();
        getInfoOptions.set_num_err_choice(NumErrorsChoice.NONE);

        return client.getTopologyInfoByNameWithOpts(
                topologyName,
                getInfoOptions
        );
    }

    public void metrics(Nimbus.Iface client) throws Exception {
        validateConfiguration();

        if (component == null || component.isEmpty()) {
            printAvailableComponents(client);
            System.out.println("Please use -m to specify one component");
            return;
        }

        validateStreamAndWatch();

        System.out.println(
                "topology\tcomponent\tparallelism\tstream\ttime-diff ms\t"
                        + watch
                        + "\tthroughput (Kt/s)"
        );

        long pollMs = interval * 1000L;
        long now = System.currentTimeMillis();

        MetricsState state = new MetricsState(now, 0);
        Poller poller = new Poller(now, pollMs);

        do {
            metrics(client, now, state);

            try {
                now = poller.nextPoll();
            } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
                break;
            }
        } while (true);
    }

    public void metrics(
            Nimbus.Iface client,
            long now,
            MetricsState state
    ) throws Exception {

        TopologyInfo info = getTopologyInfo(client, topology);
        CollectedMetrics collectedMetrics = collectComponentMetrics(info);

        validateCollectedMetrics(client, collectedMetrics);

        long timeDelta = now - state.getLastTime();
        long stattedDelta =
                collectedMetrics.getTotalStatted() - state.getLastStatted();

        state.setLastTime(now);
        state.setLastStatted(collectedMetrics.getTotalStatted());

        double throughput = calculateThroughput(
                stattedDelta,
                timeDelta
        );

        printMetrics(
                collectedMetrics.getComponentParallelism(),
                timeDelta,
                stattedDelta,
                throughput
        );
    }

    private void validateConfiguration() {
        if (interval <= 0) {
            throw new IllegalArgumentException(
                    "poll interval must be positive"
            );
        }

        if (topology == null || topology.isEmpty()) {
            throw new IllegalArgumentException(
                    "topology name must be something"
            );
        }
    }

    private void validateStreamAndWatch() {
        if (stream == null || stream.isEmpty()) {
            throw new IllegalArgumentException(
                    "stream name must be something"
            );
        }

        if (!WATCH_TRANSFERRED.equals(watch)
                && !WATCH_EMITTED.equals(watch)) {

            throw new IllegalArgumentException(
                    "watch item must either be transferred or emitted"
            );
        }
    }

    private CollectedMetrics collectComponentMetrics(TopologyInfo info) {
        CollectedMetrics collectedMetrics = new CollectedMetrics();

        for (ExecutorSummary executor : info.get_executors()) {
            collectExecutorMetrics(executor, collectedMetrics);
        }

        return collectedMetrics;
    }

    private void collectExecutorMetrics(
            ExecutorSummary executor,
            CollectedMetrics collectedMetrics
    ) {

        if (!component.equals(executor.get_component_id())) {
            return;
        }

        collectedMetrics.incrementComponentParallelism();

        Long streamValue = getStreamValue(executor.get_stats());

        if (streamValue != null) {
            collectedMetrics.addStatted(streamValue);
            collectedMetrics.setStreamFound(true);
        }
    }

    private Long getStreamValue(ExecutorStats stats) {
        if (stats == null) {
            return null;
        }

        Map<String, Map<String, Long>> watchedStats =
                getWatchedStats(stats);

        if (watchedStats == null) {
            return null;
        }

        Map<String, Long> allTimeStats =
                watchedStats.get(ALL_TIME_WINDOW);

        if (allTimeStats == null) {
            return null;
        }

        return allTimeStats.get(stream);
    }

    private Map<String, Map<String, Long>> getWatchedStats(
            ExecutorStats stats
    ) {

        if (WATCH_EMITTED.equals(watch)) {
            return stats.get_emitted();
        }

        return stats.get_transferred();
    }

    private void validateCollectedMetrics(
            Nimbus.Iface client,
            CollectedMetrics collectedMetrics
    ) throws Exception {

        if (collectedMetrics.getComponentParallelism() <= 0) {
            printAvailableComponents(client);

            throw new IllegalArgumentException(
                    "component: " + component + " not found"
            );
        }

        if (!collectedMetrics.isStreamFound()) {
            throw new IllegalArgumentException(
                    "stream: " + stream + " not found"
            );
        }
    }

    private void printAvailableComponents(Nimbus.Iface client)
            throws Exception {

        Set<String> components = getComponents(client, topology);

        System.out.println(
                "Available components for " + topology + " :"
        );
        System.out.println("------------------");

        for (String availableComponent : components) {
            System.out.println(availableComponent);
        }

        System.out.println("------------------");
    }

    private double calculateThroughput(
            long stattedDelta,
            long timeDelta
    ) {

        if (stattedDelta == 0 || timeDelta == 0) {
            return 0.0;
        }

        return (double) stattedDelta / (double) timeDelta;
    }

    private void printMetrics(
            int componentParallelism,
            long timeDelta,
            long stattedDelta,
            double throughput
    ) {

        System.out.println(
                topology + "\t"
                        + component + "\t"
                        + componentParallelism + "\t"
                        + stream + "\t"
                        + timeDelta + "\t"
                        + stattedDelta + "\t"
                        + throughput
        );
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public void setTopology(String topology) {
        this.topology = topology;
    }

    public void setComponent(String component) {
        this.component = component;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }

    public void setWatch(String watch) {
        this.watch = watch;
    }

    private static class CollectedMetrics {

        private long totalStatted;
        private int componentParallelism;
        private boolean streamFound;

        long getTotalStatted() {
            return totalStatted;
        }

        void addStatted(long statted) {
            totalStatted += statted;
        }

        int getComponentParallelism() {
            return componentParallelism;
        }

        void incrementComponentParallelism() {
            componentParallelism++;
        }

        boolean isStreamFound() {
            return streamFound;
        }

        void setStreamFound(boolean streamFound) {
            this.streamFound = streamFound;
        }
    }

    public static class MetricsState {

        private long lastTime;
        private long lastStatted;

        public MetricsState(long lastTime, long lastStatted) {
            this.lastTime = lastTime;
            this.lastStatted = lastStatted;
        }

        public long getLastStatted() {
            return lastStatted;
        }

        public void setLastStatted(long lastStatted) {
            this.lastStatted = lastStatted;
        }

        public long getLastTime() {
            return lastTime;
        }

        public void setLastTime(long lastTime) {
            this.lastTime = lastTime;
        }
    }

    private static class Poller {

        private long startTime;
        private long pollMs;

        private Poller(long startTime, long pollMs) {
            this.startTime = startTime;
            this.pollMs = pollMs;
        }

        public long nextPoll() throws InterruptedException {
            long now = System.currentTimeMillis();
            long cycle = (now - startTime) / pollMs;
            long wakeupTime = startTime + pollMs * (cycle + 1);
            long sleepTime = wakeupTime - now;

            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            }

            return System.currentTimeMillis();
        }

        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public long getPollMs() {
            return pollMs;
        }

        public void setPollMs(long pollMs) {
            this.pollMs = pollMs;
        }
    }
}