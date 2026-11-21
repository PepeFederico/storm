package org.apache.storm.utils;

import java.util.HashSet;
import java.util.Map;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GetInfoOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.NumErrorsChoice;
import org.apache.storm.generated.TopologyInfo;

public class MonitorC1 {
    private static final String WATCH_TRANSFERRED = "transferred";
    private static final String WATCH_EMITTED = "emitted";
    private static final String ALL_TIME = ":all-time";

    private int interval = 4;
    private String topology;
    private String component;
    private String stream;
    private String watch;

    private HashSet<String> getComponents(
            Nimbus.Iface client,
            String topologyName) throws Exception {

        HashSet<String> components = new HashSet<>();
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
        validateConfiguration();

        if (component == null || component.isEmpty()) {
            printAvailableComponents(client);
            System.out.println("Please use -m to specify one component");
            return;
        }

        System.out.println(
                "topology\tcomponent\tparallelism\tstream\ttime-diff ms\t"
                        + watch
                        + "\tthroughput (Kt/s)");

        /*
         * 1000L forza l'esecuzione della moltiplicazione usando long,
         * evitando che interval * 1000 venga prima calcolato come int.
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
                Thread.currentThread().interrupt();
                e.printStackTrace();
                break;
            }
        } while (true);
    }

    public void metrics(
            Nimbus.Iface client,
            long now,
            MetricsState state) throws Exception {

        TopologyInfo info = getTopologyInfo(client, topology);
        ComponentMetrics componentMetrics = collectComponentMetrics(info);

        validateCollectedMetrics(client, componentMetrics);

        long timeDelta = now - state.getLastTime();
        long stattedDelta =
                componentMetrics.getTotalStatted() - state.getLastStatted();

        updateState(state, now, componentMetrics.getTotalStatted());

        double throughput = calculateThroughput(stattedDelta, timeDelta);

        printMetrics(
                componentMetrics.getParallelism(),
                timeDelta,
                stattedDelta,
                throughput);
    }

    private void validateConfiguration() {
        if (interval <= 0) {
            throw new IllegalArgumentException(
                    "poll interval must be positive");
        }

        if (topology == null || topology.isEmpty()) {
            throw new IllegalArgumentException(
                    "topology name must be something");
        }

        /*
         * L'assenza del componente non è un errore: metrics(client)
         * mostrerà l'elenco dei componenti disponibili.
         */
        if (component == null || component.isEmpty()) {
            return;
        }

        if (stream == null || stream.isEmpty()) {
            throw new IllegalArgumentException(
                    "stream name must be something");
        }

        if (!WATCH_TRANSFERRED.equals(watch)
                && !WATCH_EMITTED.equals(watch)) {
            throw new IllegalArgumentException(
                    "watch item must either be transferred or emitted");
        }
    }

    private ComponentMetrics collectComponentMetrics(TopologyInfo info) {
        ComponentMetrics componentMetrics = new ComponentMetrics();

        for (ExecutorSummary executor : info.get_executors()) {
            if (component.equals(executor.get_component_id())) {
                collectExecutorMetrics(executor, componentMetrics);
            }
        }

        return componentMetrics;
    }

    private void collectExecutorMetrics(
            ExecutorSummary executor,
            ComponentMetrics componentMetrics) {

        componentMetrics.incrementParallelism();

        Long streamMetric = getStreamMetric(executor.get_stats());

        if (streamMetric != null) {
            componentMetrics.addToTotalStatted(streamMetric);
            componentMetrics.setStreamFound(true);
        }
    }

    private Long getStreamMetric(ExecutorStats stats) {
        if (stats == null) {
            return null;
        }

        Map<String, Map<String, Long>> statted = getSelectedStats(stats);
        if (statted == null) {
            return null;
        }

        Map<String, Long> allTimeStats = statted.get(ALL_TIME);
        if (allTimeStats == null) {
            return null;
        }

        /*
         * Il nome locale "streamMetric" non nasconde più il campo
         * di istanza "stream".
         */
        Long streamMetric = allTimeStats.get(this.stream);
        return streamMetric;
    }

    private Map<String, Map<String, Long>> getSelectedStats(
            ExecutorStats stats) {

        if (WATCH_EMITTED.equals(watch)) {
            return stats.get_emitted();
        }

        return stats.get_transferred();
    }

    private void validateCollectedMetrics(
            Nimbus.Iface client,
            ComponentMetrics componentMetrics) throws Exception {

        if (componentMetrics.getParallelism() <= 0) {
            printAvailableComponents(client);

            throw new IllegalArgumentException(
                    "component: " + component + " not found");
        }

        if (!componentMetrics.isStreamFound()) {
            throw new IllegalArgumentException(
                    "stream: " + stream + " not found");
        }
    }

    private void printAvailableComponents(
            Nimbus.Iface client) throws Exception {

        HashSet<String> components = getComponents(client, topology);

        System.out.println("Available components for " + topology + " :");
        System.out.println("------------------");

        for (String availableComponent : components) {
            System.out.println(availableComponent);
        }

        System.out.println("------------------");
    }

    private void updateState(
            MetricsState state,
            long now,
            long totalStatted) {

        state.setLastTime(now);
        state.setLastStatted(totalStatted);
    }

    private double calculateThroughput(
            long stattedDelta,
            long timeDelta) {

        if (stattedDelta == 0 || timeDelta == 0) {
            return 0.0;
        }

        return (double) stattedDelta / (double) timeDelta;
    }

    private void printMetrics(
            int componentParallelism,
            long timeDelta,
            long stattedDelta,
            double throughput) {

        System.out.println(
                topology + "\t"
                        + component + "\t"
                        + componentParallelism + "\t"
                        + stream + "\t"
                        + timeDelta + "\t"
                        + stattedDelta + "\t"
                        + throughput);
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

    /*
     * Oggetto interno che rappresenta il risultato della raccolta
     * delle metriche per il componente selezionato.
     */
    private static class ComponentMetrics {
        private long totalStatted;
        private int parallelism;
        private boolean streamFound;

        public long getTotalStatted() {
            return totalStatted;
        }

        public void addToTotalStatted(long value) {
            totalStatted += value;
        }

        public int getParallelism() {
            return parallelism;
        }

        public void incrementParallelism() {
            parallelism++;
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
            long wakeupTime = startTime + (pollMs * (cycle + 1));
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
