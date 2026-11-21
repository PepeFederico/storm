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

public class MonitorC3 {

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
            String topologyName) throws Exception {

        GetInfoOptions getInfoOptions = new GetInfoOptions();
        getInfoOptions.set_num_err_choice(NumErrorsChoice.NONE);

        return client.getTopologyInfoByNameWithOpts(
                topologyName,
                getInfoOptions);
    }

    public void metrics(Nimbus.Iface client) throws Exception {
        validateInterval();
        validateTopology();

        if (component == null || component.isEmpty()) {
            printAvailableComponents(client);
            System.out.println("Please use -m to specify one component");
            return;
        }

        validateStream();
        validateWatch();

        System.out.println(
                "topology\tcomponent\tparallelism\tstream\ttime-diff ms\t"
                        + watch
                        + "\tthroughput (Kt/s)");

        /*
         * 1000L forza la moltiplicazione a essere eseguita usando long,
         * evitando un possibile overflow durante una moltiplicazione int.
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
                /*
                 * Ripristina lo stato di interruzione del thread.
                 * Il ciclo termina in modo controllato.
                 */
                Thread.currentThread().interrupt();
                break;
            }
        } while (true);
    }

    public void metrics(
            Nimbus.Iface client,
            long now,
            MetricsState state) throws Exception {

        TopologyInfo topologyInfo = getTopologyInfo(client, topology);
        ComponentMetrics componentMetrics =
                collectComponentMetrics(topologyInfo);

        validateComponentFound(client, componentMetrics);
        validateStreamFound(componentMetrics);

        printMetrics(now, state, componentMetrics);
    }

    private ComponentMetrics collectComponentMetrics(
            TopologyInfo topologyInfo) {

        ComponentMetrics result = new ComponentMetrics();

        for (ExecutorSummary executor : topologyInfo.get_executors()) {
            collectExecutorMetrics(executor, result);
        }

        return result;
    }

    private void collectExecutorMetrics(
            ExecutorSummary executor,
            ComponentMetrics result) {

        if (!component.equals(executor.get_component_id())) {
            return;
        }

        result.incrementParallelism();

        Long streamCount = getStreamCount(executor.get_stats());

        if (streamCount != null) {
            result.addToTotalStatted(streamCount);
            result.markStreamFound();
        }
    }

    private Long getStreamCount(ExecutorStats stats) {
        if (stats == null) {
            return null;
        }

        Map<String, Map<String, Long>> statistics =
                getSelectedStatistics(stats);

        if (statistics == null) {
            return null;
        }

        Map<String, Long> allTimeStatistics =
                statistics.get(ALL_TIME_WINDOW);

        if (allTimeStatistics == null) {
            return null;
        }

        /*
         * Il nome streamCount non nasconde più il campo di istanza stream.
         */
        return allTimeStatistics.get(this.stream);
    }

    private Map<String, Map<String, Long>> getSelectedStatistics(
            ExecutorStats stats) {

        if (WATCH_EMITTED.equals(watch)) {
            return stats.get_emitted();
        }

        return stats.get_transferred();
    }

    private void validateComponentFound(
            Nimbus.Iface client,
            ComponentMetrics componentMetrics) throws Exception {

        if (componentMetrics.getParallelism() > 0) {
            return;
        }

        printAvailableComponents(client);

        throw new IllegalArgumentException(
                "component: " + component + " not found");
    }

    private void validateStreamFound(ComponentMetrics componentMetrics) {
        if (!componentMetrics.isStreamFound()) {
            throw new IllegalArgumentException(
                    "stream: " + stream + " not found");
        }
    }

    private void printMetrics(
            long now,
            MetricsState state,
            ComponentMetrics componentMetrics) {

        long totalStatted = componentMetrics.getTotalStatted();
        long timeDelta = now - state.getLastTime();
        long stattedDelta = totalStatted - state.getLastStatted();

        state.setLastTime(now);
        state.setLastStatted(totalStatted);

        double throughput = calculateThroughput(
                stattedDelta,
                timeDelta);

        System.out.println(
                topology + "\t"
                        + component + "\t"
                        + componentMetrics.getParallelism() + "\t"
                        + stream + "\t"
                        + timeDelta + "\t"
                        + stattedDelta + "\t"
                        + throughput);
    }

    private double calculateThroughput(
            long stattedDelta,
            long timeDelta) {

        if (stattedDelta == 0 || timeDelta == 0) {
            return 0.0;
        }

        return (double) stattedDelta / (double) timeDelta;
    }

    private void validateInterval() {
        if (interval <= 0) {
            throw new IllegalArgumentException(
                    "poll interval must be positive");
        }
    }

    private void validateTopology() {
        if (topology == null || topology.isEmpty()) {
            throw new IllegalArgumentException(
                    "topology name must be something");
        }
    }

    private void validateStream() {
        if (stream == null || stream.isEmpty()) {
            throw new IllegalArgumentException(
                    "stream name must be something");
        }
    }

    private void validateWatch() {
        if (!WATCH_TRANSFERRED.equals(watch)
                && !WATCH_EMITTED.equals(watch)) {

            throw new IllegalArgumentException(
                    "watch item must either be transferred or emitted");
        }
    }

    private void printAvailableComponents(Nimbus.Iface client)
            throws Exception {

        Set<String> components = getComponents(client, topology);

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

    private static class ComponentMetrics {

        private long totalStatted;
        private int parallelism;
        private boolean streamFound;

        private void addToTotalStatted(long value) {
            totalStatted += value;
        }

        private void incrementParallelism() {
            parallelism++;
        }

        private void markStreamFound() {
            streamFound = true;
        }

        private long getTotalStatted() {
            return totalStatted;
        }

        private int getParallelism() {
            return parallelism;
        }

        private boolean isStreamFound() {
            return streamFound;
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
