package org.apache.storm.utils.staticmocking.llm.guided;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GetInfoOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.utils.Monitor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/*
 * Public methods identified in Monitor:
 *
 * 1. public void metrics(Nimbus.Iface client) throws Exception
 * 2. public void metrics(Nimbus.Iface client, long now, MetricsState state)
 *        throws Exception
 * 3. public void setInterval(int interval)
 * 4. public void setTopology(String topology)
 * 5. public void setComponent(String component)
 * 6. public void setStream(String stream)
 * 7. public void setWatch(String watch)
 *
 * MetricsState is private, despite being used in the signature of a public
 * method. Consequently, reflection is required to invoke that overload from
 * this test class.
 *
 * Expert review summary:
 *
 * Expert 1 focused on normal metric aggregation and state updates.
 * Expert 2 focused on validation, missing topology components, and streams.
 * Expert 3 focused on null statistics, empty maps, output, setter behavior,
 * zero deltas, transferred/emitted selection, and interruption.
 *
 * The proposals were combined because none of the approaches contradicted
 * the implementation.
 */
class MonitorLLMGuidedP1FinalTest {

    private static final String TOPOLOGY = "test-topology";
    private static final String COMPONENT = "test-component";
    private static final String STREAM = "test-stream";

    private Monitor monitor;
    private Nimbus.Iface client;

    private PrintStream originalOut;
    private PrintStream originalErr;
    private ByteArrayOutputStream capturedOut;
    private ByteArrayOutputStream capturedErr;

    @BeforeEach
    void setUp() {
        monitor = new Monitor();
        client = mock(Nimbus.Iface.class);

        originalOut = System.out;
        originalErr = System.err;
        capturedOut = new ByteArrayOutputStream();
        capturedErr = new ByteArrayOutputStream();

        System.setOut(new PrintStream(capturedOut));
        System.setErr(new PrintStream(capturedErr));
    }

    @AfterEach
    void tearDown() {
        System.setOut(originalOut);
        System.setErr(originalErr);

        // Avoid leaking an interrupted status into another JUnit test.
        Thread.interrupted();
    }

    @Test
    void setIntervalShouldUpdateInterval() throws Exception {
        monitor.setInterval(10);

        assertEquals(10, getMonitorField("interval"));
    }

    @Test
    void setTopologyShouldUpdateTopology() throws Exception {
        monitor.setTopology(TOPOLOGY);

        assertEquals(TOPOLOGY, getMonitorField("topology"));
    }

    @Test
    void setComponentShouldUpdateComponent() throws Exception {
        monitor.setComponent(COMPONENT);

        assertEquals(COMPONENT, getMonitorField("component"));
    }

    @Test
    void setStreamShouldUpdateStream() throws Exception {
        monitor.setStream(STREAM);

        assertEquals(STREAM, getMonitorField("stream"));
    }

    @Test
    void setWatchShouldUpdateWatchItem() throws Exception {
        monitor.setWatch("emitted");

        assertEquals("emitted", getMonitorField("watch"));
    }

    @Test
    void metricsShouldRejectZeroInterval() {
        monitor.setInterval(0);
        monitor.setTopology(TOPOLOGY);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> monitor.metrics(client)
        );

        assertEquals("poll interval must be positive", exception.getMessage());
    }

    @Test
    void metricsShouldRejectNegativeInterval() {
        monitor.setInterval(-1);
        monitor.setTopology(TOPOLOGY);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> monitor.metrics(client)
        );

        assertEquals("poll interval must be positive", exception.getMessage());
    }

    @Test
    void metricsShouldRejectNullTopology() {
        monitor.setInterval(1);
        monitor.setTopology(null);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> monitor.metrics(client)
        );

        assertEquals("topology name must be something", exception.getMessage());
    }

    @Test
    void metricsShouldRejectEmptyTopology() {
        monitor.setInterval(1);
        monitor.setTopology("");

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> monitor.metrics(client)
        );

        assertEquals("topology name must be something", exception.getMessage());
    }

    @Test
    void metricsShouldListComponentsWhenComponentIsNull() throws Exception {
        monitor.setInterval(1);
        monitor.setTopology(TOPOLOGY);
        monitor.setComponent(null);

        TopologyInfo topologyInfo = topologyInfo(
                executor("component-one", null),
                executor("component-two", null),
                executor("component-one", null)
        );
        when(client.getTopologyInfoByNameWithOpts(
                any(String.class), any(GetInfoOptions.class)
        )).thenReturn(topologyInfo);

        monitor.metrics(client);

        String output = capturedOut.toString();
        assertTrue(output.contains("Available components for " + TOPOLOGY + " :"));
        assertTrue(output.contains("component-one"));
        assertTrue(output.contains("component-two"));
        assertTrue(output.contains("Please use -m to specify one component"));

        verify(client).getTopologyInfoByNameWithOpts(
                any(String.class), any(GetInfoOptions.class)
        );
    }

    @Test
    void metricsShouldListComponentsWhenComponentIsEmpty() throws Exception {
        monitor.setInterval(1);
        monitor.setTopology(TOPOLOGY);
        monitor.setComponent("");

        when(client.getTopologyInfoByNameWithOpts(
                any(String.class), any(GetInfoOptions.class)
        )).thenReturn(topologyInfo(executor("bolt", null)));

        monitor.metrics(client);

        assertTrue(capturedOut.toString().contains("bolt"));
        assertTrue(capturedOut.toString()
                .contains("Please use -m to specify one component"));
    }

    @Test
    void metricsShouldRejectNullStream() {
        configureMonitor(COMPONENT, null, "emitted");

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> monitor.metrics(client)
        );

        assertEquals("stream name must be something", exception.getMessage());
    }

    @Test
    void metricsShouldRejectEmptyStream() {
        configureMonitor(COMPONENT, "", "emitted");

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> monitor.metrics(client)
        );

        assertEquals("stream name must be something", exception.getMessage());
    }

    @Test
    void metricsShouldRejectNullWatchItem() {
        configureMonitor(COMPONENT, STREAM, null);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> monitor.metrics(client)
        );

        assertEquals(
                "watch item must either be transferred or emitted",
                exception.getMessage()
        );
    }

    @Test
    void metricsShouldRejectUnknownWatchItem() {
        configureMonitor(COMPONENT, STREAM, "acked");

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> monitor.metrics(client)
        );

        assertEquals(
                "watch item must either be transferred or emitted",
                exception.getMessage()
        );
    }

    @Test
    void metricsShouldPrintHeaderAndStopWhenPollingThreadIsInterrupted()
            throws Exception {
        configureMonitor(COMPONENT, STREAM, "emitted");

        ExecutorStats stats = emittedStats(STREAM, 100L);
        when(client.getTopologyInfoByNameWithOpts(
                any(String.class), any(GetInfoOptions.class)
        )).thenReturn(topologyInfo(executor(COMPONENT, stats)));

        Thread.currentThread().interrupt();
        monitor.metrics(client);

        String output = capturedOut.toString();
        assertTrue(output.contains(
                "topology\tcomponent\tparallelism\tstream\ttime-diff ms"
        ));
        assertTrue(output.contains("emitted\tthroughput (Kt/s)"));
        assertTrue(output.contains(TOPOLOGY));
        assertTrue(output.contains(COMPONENT));
        assertTrue(output.contains(STREAM));

        // InterruptedException clears the interrupted status.
        assertFalse(Thread.currentThread().isInterrupted());
    }

    @Test
    void metricsOverloadShouldAggregateEmittedMetricsAndUpdateState()
            throws Exception {
        configureMonitor(COMPONENT, STREAM, "emitted");

        ExecutorSummary first = executor(
                COMPONENT,
                emittedStats(STREAM, 150L)
        );
        ExecutorSummary second = executor(
                COMPONENT,
                emittedStats(STREAM, 100L)
        );
        ExecutorSummary unrelated = executor(
                "different-component",
                emittedStats(STREAM, 1000L)
        );

        when(client.getTopologyInfoByNameWithOpts(
                any(String.class), any(GetInfoOptions.class)
        )).thenReturn(topologyInfo(first, second, unrelated));

        Object state = newMetricsState(1000L, 50L);
        invokeMetricsOverload(1200L, state);

        assertEquals(1200L, invokeStateGetter(state, "getLastTime"));
        assertEquals(250L, invokeStateGetter(state, "getLastStatted"));

        String output = capturedOut.toString();
        assertTrue(output.contains(
                TOPOLOGY + "\t" + COMPONENT + "\t2\t" + STREAM
        ));
        assertTrue(output.contains("\t200\t200\t1.0"));
    }

    @Test
    void metricsOverloadShouldUseTransferredMetrics() throws Exception {
        configureMonitor(COMPONENT, STREAM, "transferred");

        ExecutorStats stats = transferredStats(STREAM, 80L);
        when(client.getTopologyInfoByNameWithOpts(
                any(String.class), any(GetInfoOptions.class)
        )).thenReturn(topologyInfo(executor(COMPONENT, stats)));

        Object state = newMetricsState(1000L, 20L);
        invokeMetricsOverload(1100L, state);

        assertEquals(80L, invokeStateGetter(state, "getLastStatted"));
        assertTrue(capturedOut.toString().contains("\t100\t60\t0.6"));
    }

    @Test
    void metricsOverloadShouldReportZeroThroughputWhenTimeDeltaIsZero()
            throws Exception {
        configureMonitor(COMPONENT, STREAM, "emitted");

        when(client.getTopologyInfoByNameWithOpts(
                any(String.class), any(GetInfoOptions.class)
        )).thenReturn(
                topologyInfo(executor(COMPONENT, emittedStats(STREAM, 100L)))
        );

        Object state = newMetricsState(1000L, 50L);
        invokeMetricsOverload(1000L, state);

        assertTrue(capturedOut.toString().contains("\t0\t50\t0.0"));
    }

    @Test
    void metricsOverloadShouldReportZeroThroughputWhenMetricDeltaIsZero()
            throws Exception {
        configureMonitor(COMPONENT, STREAM, "emitted");

        when(client.getTopologyInfoByNameWithOpts(
                any(String.class), any(GetInfoOptions.class)
        )).thenReturn(
                topologyInfo(executor(COMPONENT, emittedStats(STREAM, 100L)))
        );

        Object state = newMetricsState(1000L, 100L);
        invokeMetricsOverload(1200L, state);

        assertTrue(capturedOut.toString().contains("\t200\t0\t0.0"));
    }

    @Test
    void metricsOverloadShouldRejectMissingComponentAndListAvailableComponents()
            throws Exception {
        configureMonitor("missing-component", STREAM, "emitted");

        TopologyInfo info = topologyInfo(
                executor("spout", emittedStats(STREAM, 10L)),
                executor("bolt", emittedStats(STREAM, 20L))
        );

        // The method retrieves topology information once for metric processing
        // and once through getComponents after the component is not found.
        when(client.getTopologyInfoByNameWithOpts(
                any(String.class), any(GetInfoOptions.class)
        )).thenReturn(info);

        Object state = newMetricsState(0L, 0L);

        IllegalArgumentException exception = assertInvocationCause(
                IllegalArgumentException.class,
                () -> invokeMetricsOverload(1000L, state)
        );

        assertEquals(
                "component: missing-component not found",
                exception.getMessage()
        );

        String output = capturedOut.toString();
        assertTrue(output.contains("Available components for " + TOPOLOGY));
        assertTrue(output.contains("spout"));
        assertTrue(output.contains("bolt"));

        verify(client, times(2)).getTopologyInfoByNameWithOpts(
                any(String.class), any(GetInfoOptions.class)
        );
    }

    @Test
    void metricsOverloadShouldRejectMissingStream() throws Exception {
        configureMonitor(COMPONENT, "missing-stream", "emitted");

        when(client.getTopologyInfoByNameWithOpts(
                any(String.class), any(GetInfoOptions.class)
        )).thenReturn(
                topologyInfo(executor(COMPONENT, emittedStats(STREAM, 10L)))
        );

        Object state = newMetricsState(0L, 0L);

        IllegalArgumentException exception = assertInvocationCause(
                IllegalArgumentException.class,
                () -> invokeMetricsOverload(1000L, state)
        );

        assertEquals("stream: missing-stream not found", exception.getMessage());
    }

    @Test
    void metricsOverloadShouldRejectStreamWhenExecutorStatsAreNull()
            throws Exception {
        configureMonitor(COMPONENT, STREAM, "emitted");

        when(client.getTopologyInfoByNameWithOpts(
                any(String.class), any(GetInfoOptions.class)
        )).thenReturn(topologyInfo(executor(COMPONENT, null)));

        Object state = newMetricsState(0L, 0L);

        IllegalArgumentException exception = assertInvocationCause(
                IllegalArgumentException.class,
                () -> invokeMetricsOverload(1000L, state)
        );

        assertEquals("stream: " + STREAM + " not found", exception.getMessage());
    }

    @Test
    void metricsOverloadShouldRejectStreamWhenSelectedStatsMapIsNull()
            throws Exception {
        configureMonitor(COMPONENT, STREAM, "emitted");

        ExecutorStats stats = new ExecutorStats();
        stats.set_emitted(null);

        when(client.getTopologyInfoByNameWithOpts(
                any(String.class), any(GetInfoOptions.class)
        )).thenReturn(topologyInfo(executor(COMPONENT, stats)));

        Object state = newMetricsState(0L, 0L);

        IllegalArgumentException exception = assertInvocationCause(
                IllegalArgumentException.class,
                () -> invokeMetricsOverload(1000L, state)
        );

        assertEquals("stream: " + STREAM + " not found", exception.getMessage());
    }

    @Test
    void metricsOverloadShouldRejectStreamWhenAllTimeBucketIsMissing()
            throws Exception {
        configureMonitor(COMPONENT, STREAM, "emitted");

        ExecutorStats stats = new ExecutorStats();
        Map<String, Map<String, Long>> emitted = new HashMap<>();
        emitted.put("600", Collections.singletonMap(STREAM, 10L));
        stats.set_emitted(emitted);

        when(client.getTopologyInfoByNameWithOpts(
                any(String.class), any(GetInfoOptions.class)
        )).thenReturn(topologyInfo(executor(COMPONENT, stats)));

        Object state = newMetricsState(0L, 0L);

        IllegalArgumentException exception = assertInvocationCause(
                IllegalArgumentException.class,
                () -> invokeMetricsOverload(1000L, state)
        );

        assertEquals("stream: " + STREAM + " not found", exception.getMessage());
    }

    @Test
    void metricsOverloadShouldCountMatchingExecutorsEvenWhenSomeHaveNullStats()
            throws Exception {
        configureMonitor(COMPONENT, STREAM, "emitted");

        ExecutorSummary noStats = executor(COMPONENT, null);
        ExecutorSummary withStats = executor(
                COMPONENT,
                emittedStats(STREAM, 25L)
        );

        when(client.getTopologyInfoByNameWithOpts(
                any(String.class), any(GetInfoOptions.class)
        )).thenReturn(topologyInfo(noStats, withStats));

        Object state = newMetricsState(0L, 0L);
        invokeMetricsOverload(100L, state);

        assertTrue(capturedOut.toString().contains(
                TOPOLOGY + "\t" + COMPONENT + "\t2\t" + STREAM
        ));
        assertEquals(25L, invokeStateGetter(state, "getLastStatted"));
    }

    @Test
    void metricsOverloadShouldPassTopologyAndErrorOptionsToNimbus()
            throws Exception {
        configureMonitor(COMPONENT, STREAM, "emitted");

        when(client.getTopologyInfoByNameWithOpts(
                any(String.class), any(GetInfoOptions.class)
        )).thenReturn(
                topologyInfo(executor(COMPONENT, emittedStats(STREAM, 1L)))
        );

        Object state = newMetricsState(0L, 0L);
        invokeMetricsOverload(1L, state);

        ArgumentCaptor<String> topologyCaptor =
                ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<GetInfoOptions> optionsCaptor =
                ArgumentCaptor.forClass(GetInfoOptions.class);

        verify(client).getTopologyInfoByNameWithOpts(
                topologyCaptor.capture(),
                optionsCaptor.capture()
        );

        assertEquals(TOPOLOGY, topologyCaptor.getValue());
        assertNotNull(optionsCaptor.getValue());
        assertTrue(optionsCaptor.getValue().is_set_num_err_choice());
        assertEquals(
                org.apache.storm.generated.NumErrorsChoice.NONE,
                optionsCaptor.getValue().get_num_err_choice()
        );
    }

    private void configureMonitor(
            String component,
            String stream,
            String watch) {
        monitor.setInterval(1);
        monitor.setTopology(TOPOLOGY);
        monitor.setComponent(component);
        monitor.setStream(stream);
        monitor.setWatch(watch);
    }

    private Object getMonitorField(String fieldName) throws Exception {
        Field field = Monitor.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(monitor);
    }

    private TopologyInfo topologyInfo(ExecutorSummary... executors) {
        TopologyInfo topologyInfo = new TopologyInfo();
        topologyInfo.set_executors(List.of(executors));
        return topologyInfo;
    }

    private ExecutorSummary executor(
            String componentId,
            ExecutorStats stats) {
        ExecutorSummary summary = new ExecutorSummary();
        summary.set_component_id(componentId);
        summary.set_stats(stats);
        return summary;
    }

    private ExecutorStats emittedStats(String streamName, long value) {
        ExecutorStats stats = new ExecutorStats();

        Map<String, Long> streams = new HashMap<>();
        streams.put(streamName, value);

        Map<String, Map<String, Long>> emitted = new HashMap<>();
        emitted.put(":all-time", streams);

        stats.set_emitted(emitted);
        return stats;
    }

    private ExecutorStats transferredStats(String streamName, long value) {
        ExecutorStats stats = new ExecutorStats();

        Map<String, Long> streams = new HashMap<>();
        streams.put(streamName, value);

        Map<String, Map<String, Long>> transferred = new HashMap<>();
        transferred.put(":all-time", streams);

        stats.set_transferred(transferred);
        return stats;
    }

    private Class<?> metricsStateClass() {
        for (Class<?> nestedClass : Monitor.class.getDeclaredClasses()) {
            if ("MetricsState".equals(nestedClass.getSimpleName())) {
                return nestedClass;
            }
        }

        throw new AssertionError("Monitor.MetricsState was not found");
    }

    private Object newMetricsState(long lastTime, long lastStatted)
            throws Exception {
        Constructor<?> constructor =
                metricsStateClass().getDeclaredConstructor(long.class, long.class);
        constructor.setAccessible(true);
        return constructor.newInstance(lastTime, lastStatted);
    }

    private void invokeMetricsOverload(long now, Object state)
            throws Exception {
        Method method = Monitor.class.getDeclaredMethod(
                "metrics",
                Nimbus.Iface.class,
                long.class,
                metricsStateClass()
        );
        method.setAccessible(true);
        method.invoke(monitor, client, now, state);
    }

    private long invokeStateGetter(Object state, String methodName)
            throws Exception {
        Method getter = metricsStateClass().getDeclaredMethod(methodName);
        getter.setAccessible(true);
        return (Long) getter.invoke(state);
    }

    private <T extends Throwable> T assertInvocationCause(
            Class<T> expectedType,
            ThrowingOperation operation) {
        InvocationTargetException wrapper = assertThrows(
                InvocationTargetException.class,
                operation::run
        );

        assertNotNull(wrapper.getCause());
        assertTrue(
                expectedType.isInstance(wrapper.getCause()),
                () -> "Expected cause " + expectedType.getName()
                        + " but got " + wrapper.getCause()
        );

        return expectedType.cast(wrapper.getCause());
    }

    @FunctionalInterface
    private interface ThrowingOperation {
        void run() throws Exception;
    }
}