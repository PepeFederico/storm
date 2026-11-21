package org.apache.storm.utils.staticmocking.llm.zero;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GetInfoOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.NumErrorsChoice;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.utils.Monitor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * JUnit 5 tests for {@link Monitor}.
 *
 * <p>Reflection is used to exercise the overload that accepts the private
 * MetricsState type and to test the private MetricsState and Poller classes.</p>
 */
class MonitorLLMZeroShotP2FinalTest {

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

        System.setOut(new PrintStream(capturedOut, true));
        System.setErr(new PrintStream(capturedErr, true));

        monitor.setTopology(TOPOLOGY);
        monitor.setComponent(COMPONENT);
        monitor.setStream(STREAM);
        monitor.setWatch("emitted");
        monitor.setInterval(4);
    }

    @AfterEach
    void tearDown() {
        System.setOut(originalOut);
        System.setErr(originalErr);
        Thread.interrupted();
    }

    @Test
    void metricsShouldRejectZeroInterval() {
        monitor.setInterval(0);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> monitor.metrics(client)
        );

        assertEquals("poll interval must be positive", exception.getMessage());
    }

    @Test
    void metricsShouldRejectNegativeInterval() {
        monitor.setInterval(-1);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> monitor.metrics(client)
        );

        assertEquals("poll interval must be positive", exception.getMessage());
    }

    @Test
    void metricsShouldRejectNullTopology() {
        monitor.setTopology(null);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> monitor.metrics(client)
        );

        assertEquals("topology name must be something", exception.getMessage());
    }

    @Test
    void metricsShouldRejectEmptyTopology() {
        monitor.setTopology("");

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> monitor.metrics(client)
        );

        assertEquals("topology name must be something", exception.getMessage());
    }

    @Test
    void metricsShouldPrintAvailableComponentsWhenComponentIsNull()
            throws Exception {

        monitor.setComponent(null);
        TopologyInfo info = topologyInfo(
                executor("spout", null),
                executor("bolt", null),
                executor("spout", null)
        );
        stubTopologyInfo(info);

        monitor.metrics(client);

        String output = stdout();
        assertTrue(output.contains(
                "Available components for " + TOPOLOGY + " :"
        ));
        assertTrue(output.contains("spout"));
        assertTrue(output.contains("bolt"));
        assertTrue(output.contains(
                "Please use -m to specify one component"
        ));

        verifyGetTopologyInfoOptions();
    }

    @Test
    void metricsShouldPrintAvailableComponentsWhenComponentIsEmpty()
            throws Exception {

        monitor.setComponent("");
        TopologyInfo info = topologyInfo(executor("source", null));
        stubTopologyInfo(info);

        monitor.metrics(client);

        String output = stdout();
        assertTrue(output.contains(
                "Available components for " + TOPOLOGY + " :"
        ));
        assertTrue(output.contains("source"));
        assertTrue(output.contains(
                "Please use -m to specify one component"
        ));
    }

    @Test
    void metricsShouldRejectNullStream() {
        monitor.setStream(null);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> monitor.metrics(client)
        );

        assertEquals("stream name must be something", exception.getMessage());
    }

    @Test
    void metricsShouldRejectEmptyStream() {
        monitor.setStream("");

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> monitor.metrics(client)
        );

        assertEquals("stream name must be something", exception.getMessage());
    }

    @Test
    void metricsShouldRejectNullWatch() {
        monitor.setWatch(null);

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
    void metricsShouldRejectUnknownWatchValue() {
        monitor.setWatch("acked");

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
    void metricsShouldValidateBeforeCallingNimbus() throws Exception {
        monitor.setInterval(0);

        assertThrows(
                IllegalArgumentException.class,
                () -> monitor.metrics(client)
        );

        verify(client, never()).getTopologyInfoByNameWithOpts(
                any(String.class),
                any(GetInfoOptions.class)
        );
    }

    @Test
    void metricsSampleShouldSumEmittedCountsAcrossMatchingExecutors()
            throws Throwable {

        ExecutorStats firstStats = stats(
                allTime(STREAM, 4_000L),
                allTime(STREAM, 90_000L)
        );
        ExecutorStats secondStats = stats(
                allTime(STREAM, 2_000L),
                allTime(STREAM, 80_000L)
        );

        TopologyInfo info = topologyInfo(
                executor(COMPONENT, firstStats),
                executor(
                        "another-component",
                        stats(allTime(STREAM, 1_000L), null)
                ),
                executor(COMPONENT, secondStats)
        );
        stubTopologyInfo(info);

        Object state = newMetricsState(1_000L, 1_000L);

        invokeMetricsSample(3_000L, state);

        String output = stdout();
        assertTrue(output.contains(
                TOPOLOGY + "\t"
                        + COMPONENT + "\t"
                        + "2\t"
                        + STREAM + "\t"
                        + "2000\t"
                        + "5000\t"
                        + "2.5"
        ));

        assertEquals(3_000L, getMetricsStateLastTime(state));
        assertEquals(6_000L, getMetricsStateLastStatted(state));
        verifyGetTopologyInfoOptions();
    }

    @Test
    void metricsSampleShouldUseTransferredCountsWhenRequested()
            throws Throwable {

        monitor.setWatch("transferred");

        ExecutorStats executorStats = stats(
                allTime(STREAM, 1L),
                allTime(STREAM, 7_500L)
        );
        TopologyInfo info = topologyInfo(
                executor(COMPONENT, executorStats)
        );
        stubTopologyInfo(info);

        Object state = newMetricsState(1_000L, 500L);

        invokeMetricsSample(3_000L, state);

        assertTrue(stdout().contains(
                TOPOLOGY + "\t"
                        + COMPONENT + "\t"
                        + "1\t"
                        + STREAM + "\t"
                        + "2000\t"
                        + "7000\t"
                        + "3.5"
        ));
        assertEquals(7_500L, getMetricsStateLastStatted(state));
    }

    @Test
    void metricsSampleShouldProduceZeroThroughputWhenCounterDoesNotChange()
            throws Throwable {

        ExecutorStats executorStats = stats(
                allTime(STREAM, 100L),
                null
        );
        ExecutorSummary summary = executor(COMPONENT, executorStats);
        TopologyInfo info = topologyInfo(summary);
        stubTopologyInfo(info);

        Object state = newMetricsState(1_000L, 100L);

        invokeMetricsSample(2_000L, state);

        assertTrue(stdout().contains("\t1000\t0\t0.0"));
    }

    @Test
    void metricsSampleShouldProduceZeroThroughputWhenTimeDoesNotChange()
            throws Throwable {

        ExecutorStats executorStats = stats(
                allTime(STREAM, 300L),
                null
        );
        ExecutorSummary summary = executor(COMPONENT, executorStats);
        TopologyInfo info = topologyInfo(summary);
        stubTopologyInfo(info);

        Object state = newMetricsState(1_000L, 100L);

        invokeMetricsSample(1_000L, state);

        assertTrue(stdout().contains("\t0\t200\t0.0"));
    }

    @Test
    void metricsSampleShouldAllowNegativeCounterDelta() throws Throwable {
        ExecutorStats executorStats = stats(
                allTime(STREAM, 100L),
                null
        );
        ExecutorSummary summary = executor(COMPONENT, executorStats);
        TopologyInfo info = topologyInfo(summary);
        stubTopologyInfo(info);

        Object state = newMetricsState(1_000L, 300L);

        invokeMetricsSample(2_000L, state);

        assertTrue(stdout().contains("\t1000\t-200\t-0.2"));
    }

    @Test
    void metricsSampleShouldCountMatchingExecutorEvenWhenStatsAreNull()
            throws Throwable {

        ExecutorStats validStats = stats(
                allTime(STREAM, 10L),
                null
        );
        TopologyInfo info = topologyInfo(
                executor(COMPONENT, null),
                executor(COMPONENT, validStats)
        );
        stubTopologyInfo(info);

        Object state = newMetricsState(0L, 0L);

        invokeMetricsSample(10L, state);

        assertTrue(stdout().contains(
                TOPOLOGY + "\t" + COMPONENT + "\t2\t" + STREAM
        ));
    }

    @Test
    void metricsSampleShouldIgnoreNullStatsMapsWhenAnotherExecutorContainsStream()
            throws Throwable {

        ExecutorStats nullMapStats = stats(null, null);
        ExecutorStats validStats = stats(
                allTime(STREAM, 12L),
                null
        );
        ExecutorSummary first = executor(COMPONENT, nullMapStats);
        ExecutorSummary second = executor(COMPONENT, validStats);
        TopologyInfo info = topologyInfo(first, second);
        stubTopologyInfo(info);

        Object state = newMetricsState(0L, 0L);

        invokeMetricsSample(4L, state);

        assertEquals(12L, getMetricsStateLastStatted(state));
        assertTrue(stdout().contains("\t4\t12\t3.0"));
    }

    @Test
    void metricsSampleShouldIgnoreStatsWithoutAllTimeWindow()
            throws Throwable {

        Map<String, Map<String, Long>> noAllTime = new HashMap<>();
        noAllTime.put(
                "600",
                Collections.singletonMap(STREAM, 99L)
        );

        ExecutorStats noAllTimeStats = stats(noAllTime, null);
        ExecutorStats validStats = stats(
                allTime(STREAM, 8L),
                null
        );
        ExecutorSummary first = executor(COMPONENT, noAllTimeStats);
        ExecutorSummary second = executor(COMPONENT, validStats);
        TopologyInfo info = topologyInfo(first, second);
        stubTopologyInfo(info);

        Object state = newMetricsState(0L, 0L);

        invokeMetricsSample(2L, state);

        assertEquals(8L, getMetricsStateLastStatted(state));
        assertTrue(stdout().contains("\t2\t8\t4.0"));
    }

    @Test
    void metricsSampleShouldIgnoreAllTimeWindowWithoutRequestedStream()
            throws Throwable {

        ExecutorStats wrongStreamStats = stats(
                allTime("other-stream", 50L),
                null
        );
        ExecutorStats validStats = stats(
                allTime(STREAM, 6L),
                null
        );
        ExecutorSummary first = executor(
                COMPONENT,
                wrongStreamStats
        );
        ExecutorSummary second = executor(COMPONENT, validStats);
        TopologyInfo info = topologyInfo(first, second);
        stubTopologyInfo(info);

        Object state = newMetricsState(0L, 0L);

        invokeMetricsSample(3L, state);

        assertEquals(6L, getMetricsStateLastStatted(state));
        assertTrue(stdout().contains("\t3\t6\t2.0"));
    }

    @Test
    void metricsSampleShouldTreatZeroStreamCountAsFound()
            throws Throwable {

        ExecutorStats executorStats = stats(
                allTime(STREAM, 0L),
                null
        );
        ExecutorSummary summary = executor(COMPONENT, executorStats);
        TopologyInfo info = topologyInfo(summary);
        stubTopologyInfo(info);

        Object state = newMetricsState(100L, 0L);

        invokeMetricsSample(200L, state);

        assertTrue(stdout().contains("\t100\t0\t0.0"));
        assertEquals(0L, getMetricsStateLastStatted(state));
    }

    @Test
    void metricsSampleShouldPrintComponentsAndRejectUnknownComponent()
            throws Throwable {

        TopologyInfo info = topologyInfo(
                executor("spout", null),
                executor("bolt", null)
        );
        stubTopologyInfo(info);

        Object state = newMetricsState(100L, 25L);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> invokeMetricsSample(200L, state)
        );

        assertEquals(
                "component: " + COMPONENT + " not found",
                exception.getMessage()
        );

        String output = stdout();
        assertTrue(output.contains(
                "Available components for " + TOPOLOGY + " :"
        ));
        assertTrue(output.contains("spout"));
        assertTrue(output.contains("bolt"));

        assertEquals(100L, getMetricsStateLastTime(state));
        assertEquals(25L, getMetricsStateLastStatted(state));

        /*
         * Monitor first obtains the topology information in metrics(), then
         * obtains it again through getComponents() to produce the diagnostic
         * component list.
         */
        verify(client, times(2)).getTopologyInfoByNameWithOpts(
                eq(TOPOLOGY),
                any(GetInfoOptions.class)
        );
    }

    @Test
    void metricsSampleShouldRejectMissingStreamWhenMatchingStatsAreNull()
            throws Throwable {

        ExecutorSummary summary = executor(COMPONENT, null);
        TopologyInfo info = topologyInfo(summary);
        stubTopologyInfo(info);

        Object state = newMetricsState(100L, 10L);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> invokeMetricsSample(200L, state)
        );

        assertEquals(
                "stream: " + STREAM + " not found",
                exception.getMessage()
        );
        assertEquals(100L, getMetricsStateLastTime(state));
        assertEquals(10L, getMetricsStateLastStatted(state));
    }

    @Test
    void metricsSampleShouldRejectMissingStreamWhenStatsMapIsNull()
            throws Throwable {

        ExecutorStats executorStats = stats(null, null);
        ExecutorSummary summary = executor(COMPONENT, executorStats);
        TopologyInfo info = topologyInfo(summary);
        stubTopologyInfo(info);

        Object state = newMetricsState(0L, 0L);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> invokeMetricsSample(1L, state)
        );

        assertEquals(
                "stream: " + STREAM + " not found",
                exception.getMessage()
        );
    }

    @Test
    void metricsSampleShouldRejectMissingStreamWhenAllTimeWindowIsAbsent()
            throws Throwable {

        Map<String, Map<String, Long>> emitted = new HashMap<>();
        emitted.put(
                "600",
                Collections.singletonMap(STREAM, 5L)
        );

        ExecutorStats executorStats = stats(emitted, null);
        ExecutorSummary summary = executor(COMPONENT, executorStats);
        TopologyInfo info = topologyInfo(summary);
        stubTopologyInfo(info);

        Object state = newMetricsState(0L, 0L);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> invokeMetricsSample(1L, state)
        );

        assertEquals(
                "stream: " + STREAM + " not found",
                exception.getMessage()
        );
    }

    @Test
    void metricsSampleShouldPropagateNimbusFailure() throws Exception {
        /*
         * Mockito rejects java.lang.Exception here because the generated
         * Nimbus method does not declare it. A RuntimeException is valid and
         * still verifies that Monitor propagates Nimbus failures unchanged.
         */
        RuntimeException nimbusFailure =
                new RuntimeException("Nimbus unavailable");

        doThrow(nimbusFailure)
                .when(client)
                .getTopologyInfoByNameWithOpts(
                        eq(TOPOLOGY),
                        any(GetInfoOptions.class)
                );

        Object state = newMetricsState(0L, 0L);

        RuntimeException thrown = assertThrows(
                RuntimeException.class,
                () -> invokeMetricsSample(1L, state)
        );

        assertSame(nimbusFailure, thrown);
    }

    @Test
    void settersShouldStoreAllProvidedValues() throws Exception {
        monitor.setInterval(15);
        monitor.setTopology("new-topology");
        monitor.setComponent("new-component");
        monitor.setStream("new-stream");
        monitor.setWatch("transferred");

        assertEquals(15, readMonitorField("interval"));
        assertEquals("new-topology", readMonitorField("topology"));
        assertEquals("new-component", readMonitorField("component"));
        assertEquals("new-stream", readMonitorField("stream"));
        assertEquals("transferred", readMonitorField("watch"));
    }

    @Test
    void metricsStateConstructorAndAccessorsShouldWork()
            throws Exception {

        Object state = newMetricsState(10L, 20L);

        assertEquals(10L, getMetricsStateLastTime(state));
        assertEquals(20L, getMetricsStateLastStatted(state));

        invokeNoResult(
                state,
                "setLastTime",
                long.class,
                30L
        );
        invokeNoResult(
                state,
                "setLastStatted",
                long.class,
                40L
        );

        assertEquals(30L, getMetricsStateLastTime(state));
        assertEquals(40L, getMetricsStateLastStatted(state));
    }

    @Test
    void pollerConstructorAndAccessorsShouldWork() throws Exception {
        Object poller = newPoller(100L, 250L);

        assertEquals(
                100L,
                invokeLongGetter(poller, "getStartTime")
        );
        assertEquals(
                250L,
                invokeLongGetter(poller, "getPollMs")
        );

        invokeNoResult(
                poller,
                "setStartTime",
                long.class,
                500L
        );
        invokeNoResult(
                poller,
                "setPollMs",
                long.class,
                1_000L
        );

        assertEquals(
                500L,
                invokeLongGetter(poller, "getStartTime")
        );
        assertEquals(
                1_000L,
                invokeLongGetter(poller, "getPollMs")
        );
    }

    @Test
    @Timeout(2)
    void pollerNextPollShouldReturnCurrentOrLaterTimeWithoutSleepingForPastCycle()
            throws Throwable {

        long startTime = System.currentTimeMillis() - 10_000L;
        Object poller = newPoller(startTime, 10L);
        long before = System.currentTimeMillis();

        long result = invokePollerNextPoll(poller);

        long after = System.currentTimeMillis();
        assertTrue(result >= before);
        assertTrue(result <= after);
    }

    @Test
    @Timeout(2)
    void pollerNextPollShouldWaitUntilNextBoundary()
            throws Throwable {

        long startTime = System.currentTimeMillis();
        Object poller = newPoller(startTime, 25L);

        long result = invokePollerNextPoll(poller);

        assertTrue(result >= startTime);
        assertTrue(result - startTime >= 1L);
    }

    @Test
    void pollerNextPollShouldPropagateInterruption()
            throws Exception {

        Object poller = newPoller(
                System.currentTimeMillis(),
                5_000L
        );
        Thread.currentThread().interrupt();

        InterruptedException exception = assertThrows(
                InterruptedException.class,
                () -> invokePollerNextPoll(poller)
        );

        assertNotNull(exception);
        assertFalse(Thread.currentThread().isInterrupted());
    }

    /**
     * Uses doReturn rather than when(...).thenReturn(...), preventing nested
     * Mockito stubbing when test-data helper methods create other mocks.
     */
    private void stubTopologyInfo(TopologyInfo info) throws Exception {
        doReturn(info)
                .when(client)
                .getTopologyInfoByNameWithOpts(
                        eq(TOPOLOGY),
                        any(GetInfoOptions.class)
                );
    }

    private ExecutorSummary executor(
            String componentId,
            ExecutorStats executorStats) {

        ExecutorSummary summary = mock(ExecutorSummary.class);
        when(summary.get_component_id()).thenReturn(componentId);
        when(summary.get_stats()).thenReturn(executorStats);
        return summary;
    }

    private ExecutorStats stats(
            Map<String, Map<String, Long>> emitted,
            Map<String, Map<String, Long>> transferred) {

        ExecutorStats executorStats = mock(ExecutorStats.class);
        when(executorStats.get_emitted()).thenReturn(emitted);
        when(executorStats.get_transferred()).thenReturn(transferred);
        return executorStats;
    }

    private Map<String, Map<String, Long>> allTime(
            String streamName,
            long count) {

        Map<String, Long> streams = new HashMap<>();
        streams.put(streamName, count);

        Map<String, Map<String, Long>> windows = new HashMap<>();
        windows.put(":all-time", streams);
        return windows;
    }

    private TopologyInfo topologyInfo(
            ExecutorSummary... executors) {

        TopologyInfo info = mock(TopologyInfo.class);
        when(info.get_executors()).thenReturn(
                Arrays.asList(executors)
        );
        return info;
    }

    private void verifyGetTopologyInfoOptions() throws Exception {
        ArgumentCaptor<GetInfoOptions> optionsCaptor =
                ArgumentCaptor.forClass(GetInfoOptions.class);

        verify(client).getTopologyInfoByNameWithOpts(
                eq(TOPOLOGY),
                optionsCaptor.capture()
        );

        assertEquals(
                NumErrorsChoice.NONE,
                optionsCaptor.getValue().get_num_err_choice()
        );
    }

    private Object readMonitorField(String fieldName)
            throws Exception {

        Field field = Monitor.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(monitor);
    }

    private Class<?> metricsStateClass()
            throws ClassNotFoundException {

        return Class.forName(
                Monitor.class.getName() + "$MetricsState"
        );
    }

    private Class<?> pollerClass()
            throws ClassNotFoundException {

        return Class.forName(
                Monitor.class.getName() + "$Poller"
        );
    }

    private Object newMetricsState(
            long lastTime,
            long lastStatted) throws Exception {

        Constructor<?> constructor =
                metricsStateClass().getDeclaredConstructor(
                        long.class,
                        long.class
                );
        constructor.setAccessible(true);
        return constructor.newInstance(lastTime, lastStatted);
    }

    private long getMetricsStateLastTime(Object state)
            throws Exception {

        return invokeLongGetter(state, "getLastTime");
    }

    private long getMetricsStateLastStatted(Object state)
            throws Exception {

        return invokeLongGetter(state, "getLastStatted");
    }

    private void invokeMetricsSample(
            long now,
            Object state) throws Throwable {

        Method method = Monitor.class.getDeclaredMethod(
                "metrics",
                Nimbus.Iface.class,
                long.class,
                metricsStateClass()
        );
        method.setAccessible(true);

        try {
            method.invoke(monitor, client, now, state);
        } catch (InvocationTargetException exception) {
            throw exception.getCause();
        }
    }

    private Object newPoller(
            long startTime,
            long pollMs) throws Exception {

        Constructor<?> constructor =
                pollerClass().getDeclaredConstructor(
                        long.class,
                        long.class
                );
        constructor.setAccessible(true);
        return constructor.newInstance(startTime, pollMs);
    }

    private long invokePollerNextPoll(Object poller)
            throws Throwable {

        Method method = pollerClass().getDeclaredMethod("nextPoll");
        method.setAccessible(true);

        try {
            return ((Number) method.invoke(poller)).longValue();
        } catch (InvocationTargetException exception) {
            throw exception.getCause();
        }
    }

    private long invokeLongGetter(
            Object target,
            String methodName) throws Exception {

        Method method = target.getClass().getDeclaredMethod(
                methodName
        );
        method.setAccessible(true);
        return ((Number) method.invoke(target)).longValue();
    }

    private void invokeNoResult(
            Object target,
            String methodName,
            Class<?> parameterType,
            Object value) throws Exception {

        Method method = target.getClass().getDeclaredMethod(
                methodName,
                parameterType
        );
        method.setAccessible(true);
        method.invoke(target, value);
    }

    private String stdout() {
        return new String(
                capturedOut.toByteArray(),
                StandardCharsets.UTF_8
        );
    }
}