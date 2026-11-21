package org.apache.storm.utils.staticmocking.llm.zero;

import org.apache.storm.generated.Nimbus;
import org.apache.storm.utils.Monitor;
import org.apache.thrift.TException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * Black-box tests for the public API of {@link Monitor}.
 */
@DisplayName("Monitor public API")
class MonitorLLMZeroShotP1V2Test {

    private Monitor monitor;

    @BeforeEach
    void setUp() {
        monitor = new Monitor();
    }

    @Nested
    @DisplayName("Constructor")
    class ConstructorTests {

        @Test
        @DisplayName("creates a non-null Monitor instance")
        void constructorCreatesMonitorInstance() {
            Monitor createdMonitor = assertDoesNotThrow(Monitor::new);
            assertNotNull(createdMonitor);
        }

        @Test
        @DisplayName("creates independent Monitor instances")
        void constructorCreatesIndependentInstances() {
            Monitor first = new Monitor();
            Monitor second = new Monitor();

            assertNotNull(first);
            assertNotNull(second);
            assertNotSame(first, second);
        }
    }

    @Nested
    @DisplayName("setInterval")
    class SetIntervalTests {

        @Test
        @DisplayName("accepts a positive interval")
        void setIntervalAcceptsPositiveValue() {
            assertDoesNotThrow(() -> monitor.setInterval(10));
        }

        @Test
        @DisplayName("accepts an interval of one")
        void setIntervalAcceptsOne() {
            assertDoesNotThrow(() -> monitor.setInterval(1));
        }

        @Test
        @DisplayName("accepts zero without failing immediately")
        void setIntervalAcceptsZero() {
            assertDoesNotThrow(() -> monitor.setInterval(0));
        }

        @Test
        @DisplayName("accepts a negative value without failing immediately")
        void setIntervalAcceptsNegativeValue() {
            assertDoesNotThrow(() -> monitor.setInterval(-1));
        }

        @Test
        @DisplayName("accepts the minimum integer value")
        void setIntervalAcceptsMinimumIntegerValue() {
            assertDoesNotThrow(() -> monitor.setInterval(Integer.MIN_VALUE));
        }

        @Test
        @DisplayName("accepts the maximum integer value")
        void setIntervalAcceptsMaximumIntegerValue() {
            assertDoesNotThrow(() -> monitor.setInterval(Integer.MAX_VALUE));
        }

        @Test
        @DisplayName("can be called repeatedly")
        void setIntervalCanBeCalledRepeatedly() {
            assertDoesNotThrow(() -> {
                monitor.setInterval(1);
                monitor.setInterval(30);
                monitor.setInterval(60);
            });
        }
    }

    @Nested
    @DisplayName("setTopology")
    class SetTopologyTests {

        @Test
        @DisplayName("accepts a topology name")
        void setTopologyAcceptsName() {
            assertDoesNotThrow(() -> monitor.setTopology("word-count"));
        }

        @Test
        @DisplayName("accepts an empty topology name")
        void setTopologyAcceptsEmptyString() {
            assertDoesNotThrow(() -> monitor.setTopology(""));
        }

        @Test
        @DisplayName("accepts a blank topology name")
        void setTopologyAcceptsBlankString() {
            assertDoesNotThrow(() -> monitor.setTopology("   "));
        }

        @Test
        @DisplayName("accepts a topology name containing special characters")
        void setTopologyAcceptsSpecialCharacters() {
            assertDoesNotThrow(
                    () -> monitor.setTopology("topology-01_test.name/value")
            );
        }

        @Test
        @DisplayName("accepts a Unicode topology name")
        void setTopologyAcceptsUnicodeValue() {
            assertDoesNotThrow(
                    () -> monitor.setTopology("topologia-à-测试")
            );
        }

        @Test
        @DisplayName("accepts null without failing immediately")
        void setTopologyAcceptsNull() {
            assertDoesNotThrow(() -> monitor.setTopology(null));
        }

        @Test
        @DisplayName("can replace a previously configured topology")
        void setTopologyCanBeCalledRepeatedly() {
            assertDoesNotThrow(() -> {
                monitor.setTopology("first-topology");
                monitor.setTopology("second-topology");
            });
        }
    }

    @Nested
    @DisplayName("setComponent")
    class SetComponentTests {

        @Test
        @DisplayName("accepts a component name")
        void setComponentAcceptsName() {
            assertDoesNotThrow(() -> monitor.setComponent("count-bolt"));
        }

        @Test
        @DisplayName("accepts an empty component name")
        void setComponentAcceptsEmptyString() {
            assertDoesNotThrow(() -> monitor.setComponent(""));
        }

        @Test
        @DisplayName("accepts a blank component name")
        void setComponentAcceptsBlankString() {
            assertDoesNotThrow(() -> monitor.setComponent("   "));
        }

        @Test
        @DisplayName("accepts special characters")
        void setComponentAcceptsSpecialCharacters() {
            assertDoesNotThrow(
                    () -> monitor.setComponent("component-01_test.name/value")
            );
        }

        @Test
        @DisplayName("accepts a Unicode component name")
        void setComponentAcceptsUnicodeValue() {
            assertDoesNotThrow(
                    () -> monitor.setComponent("componente-à-测试")
            );
        }

        @Test
        @DisplayName("accepts null without failing immediately")
        void setComponentAcceptsNull() {
            assertDoesNotThrow(() -> monitor.setComponent(null));
        }

        @Test
        @DisplayName("can replace a previously configured component")
        void setComponentCanBeCalledRepeatedly() {
            assertDoesNotThrow(() -> {
                monitor.setComponent("first-component");
                monitor.setComponent("second-component");
            });
        }
    }

    @Nested
    @DisplayName("setStream")
    class SetStreamTests {

        @Test
        @DisplayName("accepts a stream name")
        void setStreamAcceptsName() {
            assertDoesNotThrow(() -> monitor.setStream("default"));
        }

        @Test
        @DisplayName("accepts an empty stream name")
        void setStreamAcceptsEmptyString() {
            assertDoesNotThrow(() -> monitor.setStream(""));
        }

        @Test
        @DisplayName("accepts a blank stream name")
        void setStreamAcceptsBlankString() {
            assertDoesNotThrow(() -> monitor.setStream("   "));
        }

        @Test
        @DisplayName("accepts special characters")
        void setStreamAcceptsSpecialCharacters() {
            assertDoesNotThrow(
                    () -> monitor.setStream("stream-01_test.name/value")
            );
        }

        @Test
        @DisplayName("accepts a Unicode stream name")
        void setStreamAcceptsUnicodeValue() {
            assertDoesNotThrow(
                    () -> monitor.setStream("flusso-à-测试")
            );
        }

        @Test
        @DisplayName("accepts null without failing immediately")
        void setStreamAcceptsNull() {
            assertDoesNotThrow(() -> monitor.setStream(null));
        }

        @Test
        @DisplayName("can replace a previously configured stream")
        void setStreamCanBeCalledRepeatedly() {
            assertDoesNotThrow(() -> {
                monitor.setStream("first-stream");
                monitor.setStream("second-stream");
            });
        }
    }

    @Nested
    @DisplayName("setWatch")
    class SetWatchTests {

        @Test
        @DisplayName("accepts a watch value")
        void setWatchAcceptsValue() {
            assertDoesNotThrow(() -> monitor.setWatch("emitted"));
        }

        @Test
        @DisplayName("accepts an empty watch value")
        void setWatchAcceptsEmptyString() {
            assertDoesNotThrow(() -> monitor.setWatch(""));
        }

        @Test
        @DisplayName("accepts a blank watch value")
        void setWatchAcceptsBlankString() {
            assertDoesNotThrow(() -> monitor.setWatch("   "));
        }

        @Test
        @DisplayName("accepts special characters")
        void setWatchAcceptsSpecialCharacters() {
            assertDoesNotThrow(
                    () -> monitor.setWatch("watch-01_test.name/value")
            );
        }

        @Test
        @DisplayName("accepts a Unicode watch value")
        void setWatchAcceptsUnicodeValue() {
            assertDoesNotThrow(
                    () -> monitor.setWatch("metrica-à-测试")
            );
        }

        @Test
        @DisplayName("accepts null without failing immediately")
        void setWatchAcceptsNull() {
            assertDoesNotThrow(() -> monitor.setWatch(null));
        }

        @Test
        @DisplayName("can replace a previously configured watch value")
        void setWatchCanBeCalledRepeatedly() {
            assertDoesNotThrow(() -> {
                monitor.setWatch("first-watch");
                monitor.setWatch("second-watch");
            });
        }
    }

    @Nested
    @DisplayName("Combined configuration")
    class CombinedConfigurationTests {

        @Test
        @DisplayName("all configuration methods can be used on one instance")
        void allConfigurationMethodsCanBeUsedTogether() {
            assertDoesNotThrow(() -> {
                monitor.setInterval(30);
                monitor.setTopology("word-count");
                monitor.setComponent("count-bolt");
                monitor.setStream("default");
                monitor.setWatch("emitted");
            });
        }

        @Test
        @DisplayName("configuration can be replaced repeatedly")
        void configurationCanBeReplacedRepeatedly() {
            assertDoesNotThrow(() -> {
                monitor.setInterval(10);
                monitor.setTopology("topology-one");
                monitor.setComponent("component-one");
                monitor.setStream("stream-one");
                monitor.setWatch("watch-one");

                monitor.setInterval(20);
                monitor.setTopology("topology-two");
                monitor.setComponent("component-two");
                monitor.setStream("stream-two");
                monitor.setWatch("watch-two");
            });
        }

        @Test
        @DisplayName("null textual configuration values are accepted together")
        void nullConfigurationValuesAreAcceptedTogether() {
            assertDoesNotThrow(() -> {
                monitor.setTopology(null);
                monitor.setComponent(null);
                monitor.setStream(null);
                monitor.setWatch(null);
            });
        }
    }

    @Nested
    @DisplayName("metrics(Nimbus.Iface)")
    class MetricsTests {

        @Test
        @DisplayName("rejects invocation when a topology has not been configured")
        void metricsRejectsNullClient() {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> monitor.metrics((Nimbus.Iface) null)
            );
        }

        @Test
        @DisplayName("does not hide a checked exception produced by Nimbus")
        void metricsDoesNotHideNimbusException() throws Exception {
            Nimbus.Iface client = mock(Nimbus.Iface.class);
            TException expected = new TException("Nimbus is unavailable");

            monitor.setTopology("word-count");

            /*
             * Monitor obtains TopologyInfo through getTopologyInfoByName().
             * Stubbing getClusterInfo() does not affect this execution path.
             */
            doThrow(expected)
                    .when(client)
                    .getTopologyInfoByName("word-count");

            TException actual = assertThrows(
                    TException.class,
                    () -> monitor.metrics(client)
            );

            assertSame(expected, actual);
        }

        @Test
        @DisplayName("does not hide a runtime exception produced by Nimbus")
        void metricsDoesNotHideNimbusRuntimeException() throws Exception {
            Nimbus.Iface client = mock(Nimbus.Iface.class);
            IllegalStateException expected =
                    new IllegalStateException("Unexpected Nimbus failure");

            monitor.setTopology("word-count");

            doThrow(expected)
                    .when(client)
                    .getTopologyInfoByName("word-count");

            IllegalStateException actual = assertThrows(
                    IllegalStateException.class,
                    () -> monitor.metrics(client)
            );

            assertSame(expected, actual);
        }

        @Test
        @DisplayName("propagates interruption-related failures from Nimbus")
        void metricsPropagatesInterruptionRelatedFailure() throws Exception {
            Nimbus.Iface client = mock(Nimbus.Iface.class);
            TException expected =
                    new TException(new InterruptedException("interrupted"));

            monitor.setTopology("word-count");

            doThrow(expected)
                    .when(client)
                    .getTopologyInfoByName("word-count");

            TException actual = assertThrows(
                    TException.class,
                    () -> monitor.metrics(client)
            );

            assertSame(expected, actual);
            assertNotNull(actual.getCause());
            assertSame(InterruptedException.class, actual.getCause().getClass());
        }

        @Test
        @DisplayName("reports failure when invoked with an unconfigured mock")
        void metricsWithUnconfiguredClientDoesNotSilentlyComplete() {
            Nimbus.Iface client = mock(Nimbus.Iface.class);

            Executable invocation = () -> monitor.metrics(client);

            assertThrows(Exception.class, invocation);
        }
    }

    @Nested
    @DisplayName("metrics(Nimbus.Iface, long, MetricsState)")
    class MetricsWithStateTests {

        @Test
        @DisplayName("rejects a null client when state is also null")
        void metricsWithStateRejectsNullClient() {
            assertThrows(
                    NullPointerException.class,
                    () -> monitor.metrics(null, 0L, null)
            );
        }

        @Test
        @DisplayName("rejects a null client for a positive timestamp")
        void metricsWithStateRejectsNullClientForPositiveTimestamp() {
            assertThrows(
                    NullPointerException.class,
                    () -> monitor.metrics(null, 1L, null)
            );
        }

        @Test
        @DisplayName("rejects a null client for a negative timestamp")
        void metricsWithStateRejectsNullClientForNegativeTimestamp() {
            assertThrows(
                    NullPointerException.class,
                    () -> monitor.metrics(null, -1L, null)
            );
        }

        @Test
        @DisplayName("does not accept both a usable client and null state as a valid invocation")
        void metricsWithStateRejectsNullState() {
            Nimbus.Iface client = mock(Nimbus.Iface.class);

            assertThrows(
                    Exception.class,
                    () -> monitor.metrics(client, 1_000L, null)
            );
        }

        @Test
        @DisplayName("handles Long.MIN_VALUE without silently accepting null state")
        void metricsWithStateHandlesMinimumTimestampInput() {
            Nimbus.Iface client = mock(Nimbus.Iface.class);

            assertThrows(
                    Exception.class,
                    () -> monitor.metrics(client, Long.MIN_VALUE, null)
            );
        }

        @Test
        @DisplayName("handles Long.MAX_VALUE without silently accepting null state")
        void metricsWithStateHandlesMaximumTimestampInput() {
            Nimbus.Iface client = mock(Nimbus.Iface.class);

            assertThrows(
                    Exception.class,
                    () -> monitor.metrics(client, Long.MAX_VALUE, null)
            );
        }
    }
}