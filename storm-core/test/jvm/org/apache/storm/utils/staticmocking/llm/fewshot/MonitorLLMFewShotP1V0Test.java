package org.apache.storm.utils.staticmocking.llm.fewshot;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GetInfoOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.utils.Monitor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test suite for {@link Monitor}.
 *
 * <p>The overload {@code metrics(Nimbus.Iface, long, MetricsState)} cannot be
 * invoked directly because {@code MetricsState} is a private nested class.
 * Therefore, reflection is used to exercise that public overload without
 * changing the production code.</p>
 */
@ExtendWith(MockitoExtension.class)
@Timeout(15)
class MonitorLLMFewShotP1V0Test {

    private static final String TOPOLOGY = "topo-test";
    private static final String COMPONENT = "comp-1";
    private static final String STREAM = "default";
    private static final String WINDOW = ":all-time";

    @Mock
    private Nimbus.Iface nimbusMock;

    private Monitor monitor;

    private PrintStream originalOut;
    private PrintStream originalErr;
    private ByteArrayOutputStream capturedOut;
    private ByteArrayOutputStream capturedErr;

    @BeforeEach
    void setUp() {
        monitor = new Monitor();

        originalOut = System.out;
        originalErr = System.err;
        capturedOut = new ByteArrayOutputStream();
        capturedErr = new ByteArrayOutputStream();

        System.setOut(new PrintStream(capturedOut, true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(capturedErr, true, StandardCharsets.UTF_8));
    }

    @AfterEach
    void tearDown() {
        System.setOut(originalOut);
        System.setErr(originalErr);
        Thread.interrupted();
    }

    @ParameterizedTest(name = "TC_MON_01: interval={0}")
    @ValueSource(ints = {0, -1, -100})
    @DisplayName("TC_MON_01: setInterval configura un intervallo non positivo")
    void tcMon01_nonPositiveInterval(int interval) {
        configureMonitor(COMPONENT, STREAM, "emitted");
        monitor.setInterval(interval);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> monitor.metrics(nimbusMock));

        assertTrue(exception.getMessage().contains("poll interval must be positive"));
    }

    @ParameterizedTest(name = "TC_MON_02: topology=[{0}]")
    @NullAndEmptySource
    @DisplayName("TC_MON_02: setTopology configura un nome topologia assente")
    void tcMon02_missingTopology(String topology) {
        monitor.setInterval(1);
        monitor.setTopology(topology);
        monitor.setComponent(COMPONENT);
        monitor.setStream(STREAM);
        monitor.setWatch("emitted");

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> monitor.metrics(nimbusMock));

        assertTrue(exception.getMessage().contains("topology name must be something"));
    }

    @Test
    @DisplayName("TC_MON_03: setComponent(null) mostra i componenti disponibili")
    void tcMon03_nullComponentListsAvailableComponents() throws Exception {
        monitor.setInterval(1);
        monitor.setTopology(TOPOLOGY);
        monitor.setComponent(null);
        monitor.setStream(STREAM);
        monitor.setWatch("emitted");

        stubTopologyInfo(
                executor("comp-1", null),
                executor("comp-2", null),
                executor("comp-1", null));

        assertDoesNotThrow(() -> monitor.metrics(nimbusMock));

        String output = stdout();
        assertTrue(output.contains("Available components for " + TOPOLOGY));
        assertTrue(output.contains("comp-1"));
        assertTrue(output.contains("comp-2"));
        assertTrue(output.contains("Please use -m to specify one component"));

        verify(nimbusMock).getTopologyInfoByNameWithOpts(eq(TOPOLOGY), any(GetInfoOptions.class));
    }

    @Test
    @DisplayName("TC_MON_04: setComponent(\"\") mostra i componenti disponibili")
    void tcMon04_emptyComponentListsAvailableComponents() throws Exception {
        monitor.setInterval(1);
        monitor.setTopology(TOPOLOGY);
        monitor.setComponent("");
        monitor.setStream(STREAM);
        monitor.setWatch("emitted");

        stubTopologyInfo(executor("bolt-a", null), executor("bolt-b", null));

        assertDoesNotThrow(() -> monitor.metrics(nimbusMock));

        String output = stdout();
        assertTrue(output.contains("bolt-a"));
        assertTrue(output.contains("bolt-b"));
        assertTrue(output.contains("Please use -m to specify one component"));
    }

    @ParameterizedTest(name = "TC_MON_05: stream=[{0}]")
    @NullAndEmptySource
    @DisplayName("TC_MON_05: setStream configura un nome stream assente")
    void tcMon05_missingStream(String stream) {
        monitor.setInterval(1);
        monitor.setTopology(TOPOLOGY);
        monitor.setComponent(COMPONENT);
        monitor.setStream(stream);
        monitor.setWatch("emitted");

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> monitor.metrics(nimbusMock));

        assertTrue(exception.getMessage().contains("stream name must be something"));
    }

    @ParameterizedTest(name = "TC_MON_06: watch=[{0}]")
    @NullAndEmptySource
    @ValueSource(strings = {"invalid-metric-type", "EMITTED", "transferred "})
    @DisplayName("TC_MON_06: setWatch configura un watch item non supportato")
    void tcMon06_invalidWatchItem(String watch) {
        monitor.setInterval(1);
        monitor.setTopology(TOPOLOGY);
        monitor.setComponent(COMPONENT);
        monitor.setStream(STREAM);
        monitor.setWatch(watch);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> monitor.metrics(nimbusMock));

        assertTrue(exception.getMessage()
                .contains("watch item must either be transferred or emitted"));
    }

    @ParameterizedTest(name = "TC_MON_07: watch={0}")
    @ValueSource(strings = {"emitted", "transferred"})
    @DisplayName("TC_MON_07: metrics(client) esegue il polling per entrambi i watch supportati")
    void tcMon07_publicPollingApi(String watch) throws Exception {
        configureMonitor(COMPONENT, STREAM, watch);
        monitor.setInterval(1);

        Map<String, Map<String, Long>> metrics = metrics(STREAM, 5000L);
        stubTopologyInfo(executor(COMPONENT, stats(watch, metrics)));

        AtomicReference<Throwable> thrown = new AtomicReference<>();

        Thread poller = new Thread(() -> {
            try {
                monitor.metrics(nimbusMock);
            } catch (Throwable throwable) {
                thrown.set(throwable);
            }
        }, "monitor-test-poller");

        poller.start();

        try {
            verify(nimbusMock, timeout(4000).atLeast(2))
                    .getTopologyInfoByNameWithOpts(
                            eq(TOPOLOGY),
                            any(GetInfoOptions.class));
        } finally {
            poller.interrupt();
            poller.join(5000);
        }

        assertFalse(
                poller.isAlive(),
                "Il thread di polling deve terminare dopo l'interrupt.");

        assertTrue(
                thrown.get() == null || thrown.get() instanceof InterruptedException,
                "Terminazione inattesa: " + thrown.get());

        String output = stdout();
        assertTrue(output.contains(
                "topology\tcomponent\tparallelism\tstream\ttime-diff ms\t"
                        + watch
                        + "\tthroughput (Kt/s)"));
        assertTrue(output.contains(TOPOLOGY + "\t" + COMPONENT));
    }

    @ParameterizedTest(name = "TC_MON_08: watch={0}")
    @ValueSource(strings = {"emitted", "transferred"})
    @DisplayName("TC_MON_08: metrics(client, now, state) aggrega più executor")
    void tcMon08_metricsAggregatesMatchingExecutors(String watch) throws Exception {
        configureMonitor(COMPONENT, STREAM, watch);

        stubTopologyInfo(
                executor(COMPONENT, stats(watch, metrics(STREAM, 120L))),
                executor(COMPONENT, stats(watch, metrics(STREAM, 80L))),
                executor("other-component", stats(watch, metrics(STREAM, 1000L))));

        Object state = newMetricsState(1_000L, 50L);

        invokeMetricsOverload(1_500L, state);

        String output = stdout();
        assertTrue(output.contains(
                TOPOLOGY + "\t"
                        + COMPONENT + "\t"
                        + "2\t"
                        + STREAM + "\t"
                        + "500\t"
                        + "150\t"
                        + "0.3"));

        assertEquals(1_500L, readStateLong(state, "getLastTime"));
        assertEquals(200L, readStateLong(state, "getLastStatted"));
    }

    @ParameterizedTest(name = "TC_MON_09: watch={0}")
    @ValueSource(strings = {"emitted", "transferred"})
    @DisplayName("TC_MON_09: metriche nulle non rendono disponibile lo stream")
    void tcMon09_nullExecutorStats(String watch) throws Exception {
        configureMonitor(COMPONENT, STREAM, watch);
        stubTopologyInfo(executor(COMPONENT, null));

        Object state = newMetricsState(1_000L, 0L);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> invokeMetricsOverload(2_000L, state));

        assertTrue(exception.getMessage().contains("stream: " + STREAM + " not found"));
    }

    @ParameterizedTest(name = "TC_MON_10: watch={0}")
    @ValueSource(strings = {"emitted", "transferred"})
    @DisplayName("TC_MON_10: mappa delle metriche nulla")
    void tcMon10_nullMetricMap(String watch) throws Exception {
        configureMonitor(COMPONENT, STREAM, watch);
        stubTopologyInfo(executor(COMPONENT, stats(watch, null)));

        Object state = newMetricsState(1_000L, 0L);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> invokeMetricsOverload(2_000L, state));

        assertTrue(exception.getMessage().contains("stream: " + STREAM + " not found"));
    }

    @ParameterizedTest(name = "TC_MON_11: watch={0}")
    @ValueSource(strings = {"emitted", "transferred"})
    @DisplayName("TC_MON_11: finestra all-time assente")
    void tcMon11_allTimeWindowMissing(String watch) throws Exception {
        configureMonitor(COMPONENT, STREAM, watch);

        Map<String, Map<String, Long>> metrics = new HashMap<>();
        metrics.put("600", new HashMap<>(Map.of(STREAM, 10L)));

        stubTopologyInfo(executor(COMPONENT, stats(watch, metrics)));

        Object state = newMetricsState(1_000L, 0L);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> invokeMetricsOverload(2_000L, state));

        assertTrue(exception.getMessage().contains("stream: " + STREAM + " not found"));
    }

    @ParameterizedTest(name = "TC_MON_12: watch={0}")
    @ValueSource(strings = {"emitted", "transferred"})
    @DisplayName("TC_MON_12: stream richiesto assente tra quelli del componente")
    void tcMon12_streamNotFound(String watch) throws Exception {
        configureMonitor(COMPONENT, "missing-stream", watch);

        Map<String, Map<String, Long>> metrics = metrics("other-stream", 1L);
        stubTopologyInfo(executor(COMPONENT, stats(watch, metrics)));

        Object state = newMetricsState(1_000L, 0L);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> invokeMetricsOverload(2_000L, state));

        assertTrue(exception.getMessage().contains("stream: missing-stream not found"));
    }

    @ParameterizedTest(name = "TC_MON_13: watch={0}")
    @ValueSource(strings = {"emitted", "transferred"})
    @DisplayName("TC_MON_13: componente richiesto assente")
    void tcMon13_componentNotFound(String watch) throws Exception {
        configureMonitor("missing-component", STREAM, watch);

        stubTopologyInfo(
                executor("comp-a", stats(watch, metrics(STREAM, 10L))),
                executor("comp-b", stats(watch, metrics(STREAM, 20L))));

        Object state = newMetricsState(1_000L, 0L);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> invokeMetricsOverload(2_000L, state));

        assertTrue(exception.getMessage()
                .contains("component: missing-component not found"));

        String output = stdout();
        assertTrue(output.contains("Available components for " + TOPOLOGY));
        assertTrue(output.contains("comp-a"));
        assertTrue(output.contains("comp-b"));

        verify(nimbusMock, atLeast(2))
                .getTopologyInfoByNameWithOpts(
                        eq(TOPOLOGY),
                        any(GetInfoOptions.class));
    }

    @Test
    @DisplayName("TC_MON_14: lista executor vuota")
    void tcMon14_emptyExecutorList() throws Exception {
        configureMonitor(COMPONENT, STREAM, "emitted");
        stubTopologyInfo();

        Object state = newMetricsState(1_000L, 0L);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> invokeMetricsOverload(2_000L, state));

        assertTrue(exception.getMessage().contains("component: " + COMPONENT + " not found"));
    }

    @Test
    @DisplayName("TC_MON_15: delta temporale uguale a zero produce throughput zero")
    void tcMon15_zeroTimeDeltaProducesZeroThroughput() throws Exception {
        configureMonitor(COMPONENT, STREAM, "emitted");
        stubTopologyInfo(executor(
                COMPONENT,
                stats("emitted", metrics(STREAM, 200L))));

        Object state = newMetricsState(1_000L, 100L);

        invokeMetricsOverload(1_000L, state);

        String output = stdout();
        assertTrue(output.contains(
                TOPOLOGY + "\t"
                        + COMPONENT + "\t"
                        + "1\t"
                        + STREAM + "\t"
                        + "0\t"
                        + "100\t"
                        + "0.0"));
    }

    @Test
    @DisplayName("TC_MON_16: delta dei messaggi uguale a zero produce throughput zero")
    void tcMon16_zeroStattedDeltaProducesZeroThroughput() throws Exception {
        configureMonitor(COMPONENT, STREAM, "transferred");
        stubTopologyInfo(executor(
                COMPONENT,
                stats("transferred", metrics(STREAM, 200L))));

        Object state = newMetricsState(1_000L, 200L);

        invokeMetricsOverload(1_500L, state);

        String output = stdout();
        assertTrue(output.contains(
                TOPOLOGY + "\t"
                        + COMPONENT + "\t"
                        + "1\t"
                        + STREAM + "\t"
                        + "500\t"
                        + "0\t"
                        + "0.0"));
    }

    @Test
    @DisplayName("TC_MON_17: delta negativo viene mantenuto e calcolato")
    void tcMon17_negativeCounterDeltaIsCalculated() throws Exception {
        configureMonitor(COMPONENT, STREAM, "emitted");
        stubTopologyInfo(executor(
                COMPONENT,
                stats("emitted", metrics(STREAM, 50L))));

        Object state = newMetricsState(1_000L, 100L);

        invokeMetricsOverload(1_500L, state);

        String output = stdout();
        assertTrue(output.contains("\t500\t-50\t-0.1"));
        assertEquals(50L, readStateLong(state, "getLastStatted"));
    }

    @Test
    @DisplayName("TC_MON_18: le metriche emitted non usano transferred")
    void tcMon18_emittedWatchSelectsEmittedMap() throws Exception {
        configureMonitor(COMPONENT, STREAM, "emitted");

        ExecutorStats executorStats = mock(ExecutorStats.class);
        when(executorStats.get_emitted()).thenReturn(metrics(STREAM, 300L));
        when(executorStats.get_transferred()).thenReturn(metrics(STREAM, 999L));

        stubTopologyInfo(executor(COMPONENT, executorStats));

        Object state = newMetricsState(1_000L, 0L);
        invokeMetricsOverload(2_000L, state);

        assertEquals(300L, readStateLong(state, "getLastStatted"));
        verify(executorStats).get_emitted();
    }

    @Test
    @DisplayName("TC_MON_19: le metriche transferred non usano emitted")
    void tcMon19_transferredWatchSelectsTransferredMap() throws Exception {
        configureMonitor(COMPONENT, STREAM, "transferred");

        ExecutorStats executorStats = mock(ExecutorStats.class);
        when(executorStats.get_emitted()).thenReturn(metrics(STREAM, 999L));
        when(executorStats.get_transferred()).thenReturn(metrics(STREAM, 400L));

        stubTopologyInfo(executor(COMPONENT, executorStats));

        Object state = newMetricsState(1_000L, 0L);
        invokeMetricsOverload(2_000L, state);

        assertEquals(400L, readStateLong(state, "getLastStatted"));
        verify(executorStats).get_transferred();
    }

    @Test
    @DisplayName("TC_MON_20: tutti i setter pubblici accettano valori validi")
    void tcMon20_allPublicSettersWithValidValues() throws Exception {
        assertDoesNotThrow(() -> monitor.setInterval(1));
        assertDoesNotThrow(() -> monitor.setTopology(TOPOLOGY));
        assertDoesNotThrow(() -> monitor.setComponent(COMPONENT));
        assertDoesNotThrow(() -> monitor.setStream(STREAM));
        assertDoesNotThrow(() -> monitor.setWatch("emitted"));

        stubTopologyInfo(executor(
                COMPONENT,
                stats("emitted", metrics(STREAM, 10L))));

        Object state = newMetricsState(1_000L, 0L);

        assertDoesNotThrow(() -> invokeMetricsOverload(2_000L, state));
        assertTrue(stdout().contains(TOPOLOGY + "\t" + COMPONENT));
    }

    @Test
    @DisplayName("TC_MON_21: eccezione Nimbus propagata da metrics(client, now, state)")
    void tcMon21_nimbusExceptionIsPropagated() throws Exception {
        configureMonitor(COMPONENT, STREAM, "emitted");

        Exception nimbusFailure = new Exception("Nimbus unavailable");
        when(nimbusMock.getTopologyInfoByNameWithOpts(
                eq(TOPOLOGY),
                any(GetInfoOptions.class)))
                .thenThrow(nimbusFailure);

        Object state = newMetricsState(1_000L, 0L);

        Exception actual = assertThrows(
                Exception.class,
                () -> invokeMetricsOverload(2_000L, state));

        assertSame(actual, nimbusFailure);
    }

    @Test
    @DisplayName("TC_MON_22: TopologyInfo con executor null propaga NullPointerException")
    void tcMon22_nullExecutorsList() throws Exception {
        configureMonitor(COMPONENT, STREAM, "emitted");

        TopologyInfo topologyInfo = mock(TopologyInfo.class);
        when(topologyInfo.get_executors()).thenReturn(null);
        when(nimbusMock.getTopologyInfoByNameWithOpts(
                eq(TOPOLOGY),
                any(GetInfoOptions.class)))
                .thenReturn(topologyInfo);

        Object state = newMetricsState(1_000L, 0L);

        assertThrows(
                NullPointerException.class,
                () -> invokeMetricsOverload(2_000L, state));
    }

    private void configureMonitor(String component, String stream, String watch) {
        monitor.setInterval(1);
        monitor.setTopology(TOPOLOGY);
        monitor.setComponent(component);
        monitor.setStream(stream);
        monitor.setWatch(watch);
    }

    private Map<String, Map<String, Long>> metrics(
            String streamName,
            long value) {

        Map<String, Long> streamValues = new HashMap<>();
        streamValues.put(streamName, value);

        Map<String, Map<String, Long>> result = new HashMap<>();
        result.put(WINDOW, streamValues);
        return result;
    }

    private ExecutorStats stats(
            String watch,
            Map<String, Map<String, Long>> values) {

        ExecutorStats executorStats = mock(ExecutorStats.class);

        if ("emitted".equals(watch)) {
            when(executorStats.get_emitted()).thenReturn(values);
        } else {
            when(executorStats.get_transferred()).thenReturn(values);
        }

        return executorStats;
    }

    private ExecutorSummary executor(
            String componentId,
            ExecutorStats executorStats) {

        ExecutorSummary executorSummary = mock(ExecutorSummary.class);
        when(executorSummary.get_component_id()).thenReturn(componentId);
        when(executorSummary.get_stats()).thenReturn(executorStats);
        return executorSummary;
    }

    private void stubTopologyInfo(ExecutorSummary... executors) throws Exception {
        TopologyInfo topologyInfo = mock(TopologyInfo.class);
        List<ExecutorSummary> executorList = executors.length == 0
                ? Collections.emptyList()
                : List.of(executors);

        when(topologyInfo.get_executors()).thenReturn(executorList);
        when(nimbusMock.getTopologyInfoByNameWithOpts(
                eq(TOPOLOGY),
                any(GetInfoOptions.class)))
                .thenReturn(topologyInfo);
    }

    private Object newMetricsState(
            long lastTime,
            long lastStatted) throws Exception {

        Class<?> metricsStateClass =
                Class.forName("org.apache.storm.utils.Monitor$MetricsState");

        Constructor<?> constructor =
                metricsStateClass.getDeclaredConstructor(long.class, long.class);
        constructor.setAccessible(true);

        return constructor.newInstance(lastTime, lastStatted);
    }

    private void invokeMetricsOverload(
            long now,
            Object state) throws Exception {

        Method method = Monitor.class.getDeclaredMethod(
                "metrics",
                Nimbus.Iface.class,
                long.class,
                state.getClass());
        method.setAccessible(true);

        try {
            method.invoke(monitor, nimbusMock, now, state);
        } catch (InvocationTargetException exception) {
            Throwable cause = exception.getCause();

            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }

            throw new RuntimeException(cause);
        }
    }

    private long readStateLong(
            Object state,
            String getterName) throws Exception {

        Method getter = state.getClass().getDeclaredMethod(getterName);
        getter.setAccessible(true);

        Object result = getter.invoke(state);
        assertNotNull(result);
        assertInstanceOf(Long.class, result);
        return (Long) result;
    }

    private String stdout() {
        return capturedOut.toString(StandardCharsets.UTF_8);
    }
}