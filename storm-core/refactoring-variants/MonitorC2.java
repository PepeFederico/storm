/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GetInfoOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.NumErrorsChoice;
import org.apache.storm.generated.TopologyInfo;

public class MonitorC2 {

    private static final String WATCH_TRANSFERRED = "transferred";
    private static final String WATCH_EMITTED = "emitted";
    private static final String ALL_TIME_WINDOW = ":all-time";

    private int interval = 4;
    private String topology;
    private String component;
    private String stream;
    private String watch;

    private HashSet<String> getComponents(Nimbus.Iface client, String topology)
            throws Exception {

        HashSet<String> components = new HashSet<>();
        TopologyInfo info = getTopologyInfo(client, topology);

        for (ExecutorSummary executor : info.get_executors()) {
            components.add(executor.get_component_id());
        }

        return components;
    }

    private TopologyInfo getTopologyInfo(Nimbus.Iface client, String topologyName)
            throws Exception {

        GetInfoOptions getInfoOpts = new GetInfoOptions();
        getInfoOpts.set_num_err_choice(NumErrorsChoice.NONE);

        return client.getTopologyInfoByNameWithOpts(topologyName, getInfoOpts);
    }

    public void metrics(Nimbus.Iface client) throws Exception {
        validateConfiguration();

        if (component == null || component.isEmpty()) {
            printAvailableComponents(client);
            System.out.println("Please use -m to specify one component");
            return;
        }

        System.out.println(
                "topology\tcomponent\tparallelism\tstream\ttime-diff ms\t"
                        + watch
                        + "\tthroughput (Kt/s)"
        );

        /*
         * Il literal 1000L forza la moltiplicazione a essere eseguita come long,
         * evitando che l'operazione venga prima valutata come int.
         */
        long pollMs = interval * 1000L;
        long now = System.currentTimeMillis();

        MetricsState state = new MetricsState(now, 0);
        Poller poller = new Poller(now, pollMs);

        do {
            metrics(client, now, state);

            try {
                now = poller.nextPoll();
            } catch (InterruptedException e) {
                e.printStackTrace();
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
        MetricsSnapshot snapshot = collectMetrics(info);

        validateSnapshot(client, snapshot);

        long timeDelta = now - state.getLastTime();
        long stattedDelta =
                snapshot.getTotalStatted() - state.getLastStatted();

        state.setLastTime(now);
        state.setLastStatted(snapshot.getTotalStatted());

        double throughput = calculateThroughput(
                stattedDelta,
                timeDelta
        );

        printMetrics(
                snapshot.getComponentParallelism(),
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

        /*
         * La validazione di component deve precedere quelle di stream e watch:
         * component null o vuoto attiva la modalità discovery.
         */
        if (component == null || component.isEmpty()) {
            return;
        }

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

    private MetricsSnapshot collectMetrics(TopologyInfo info) {
        MetricsSnapshot snapshot = new MetricsSnapshot();

        for (ExecutorSummary executor : info.get_executors()) {
            collectExecutorMetrics(executor, snapshot);
        }

        return snapshot;
    }

    private void collectExecutorMetrics(
            ExecutorSummary executor,
            MetricsSnapshot snapshot
    ) {
        if (!component.equals(executor.get_component_id())) {
            return;
        }

        snapshot.incrementComponentParallelism();

        Long streamValue = getStreamValue(executor.get_stats());

        if (streamValue != null) {
            snapshot.addStatted(streamValue);
            snapshot.setStreamFound(true);
        }
    }

    private Long getStreamValue(ExecutorStats stats) {
        if (stats == null) {
            return null;
        }

        Map<String, Map<String, Long>> statted = getSelectedStatistics(stats);

        if (statted == null) {
            return null;
        }

        Map<String, Long> allTimeStatistics =
                statted.get(ALL_TIME_WINDOW);

        if (allTimeStatistics == null) {
            return null;
        }

        /*
         * Il nome streamValue non nasconde più il campo di istanza stream.
         */
        return allTimeStatistics.get(this.stream);
    }

    private Map<String, Map<String, Long>> getSelectedStatistics(
            ExecutorStats stats
    ) {
        if (WATCH_EMITTED.equals(watch)) {
            return stats.get_emitted();
        }

        return stats.get_transferred();
    }

    private void validateSnapshot(
            Nimbus.Iface client,
            MetricsSnapshot snapshot
    ) throws Exception {

        if (snapshot.getComponentParallelism() <= 0) {
            printAvailableComponents(client);

            throw new IllegalArgumentException(
                    "component: " + component + " not found"
            );
        }

        if (!snapshot.isStreamFound()) {
            throw new IllegalArgumentException(
                    "stream: " + stream + " not found"
            );
        }
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

    private void printAvailableComponents(Nimbus.Iface client)
            throws Exception {

        HashSet<String> components = getComponents(client, topology);

        System.out.println("Available components for " + topology + " :");
        System.out.println("------------------");

        for (String availableComponent : components) {
            System.out.println(availableComponent);
        }

        System.out.println("------------------");
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

    private static class MetricsSnapshot {

        private long totalStatted;
        private int componentParallelism;
        private boolean streamFound;

        public long getTotalStatted() {
            return totalStatted;
        }

        public void addStatted(long statted) {
            totalStatted += statted;
        }

        public int getComponentParallelism() {
            return componentParallelism;
        }

        public void incrementComponentParallelism() {
            componentParallelism++;
        }

        public boolean isStreamFound() {
            return streamFound;
        }

        public void setStreamFound(boolean streamFound) {
            this.streamFound = streamFound;
        }
    }

    private static class MetricsState {

        private long lastTime;
        private long lastStatted;

        private MetricsState(long lastTime, long lastStatted) {
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
