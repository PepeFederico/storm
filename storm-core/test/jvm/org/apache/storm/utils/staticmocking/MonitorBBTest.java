package org.apache.storm.utils.staticmocking;

import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.utils.Monitor;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Suite Black-Box (Category Partition + Boundary Value Analysis) per {@link Monitor}.
 * Fase 1: partizioni e valori limite derivati dal contratto CLI.
 * I mock di Nimbus controllano solo cio' che i collaboratori restituiscono.
 */
@Tag("black-box")
public class MonitorBBTest {

    // Chiave della finestra temporale nelle statistiche Thrift (solo setup dei mock)
    private static final String WINDOW = ":all-time";

    private Monitor monitor;
    private Nimbus.Iface nimbusMock;

    @BeforeEach
    void setUp() {
        monitor = new Monitor();
        nimbusMock = Mockito.mock(Nimbus.Iface.class);
    }

    // ---------------------------------------------------------------------
    // Validazione parametri (metrics(client))
    // ---------------------------------------------------------------------

    /**
     * Test parametrico, nella quale è responsabilità di JUnit eseguire un caso di test per ogni parametro passato
     * In questo caso testiamo i BVA, in particolare 0 e -1, per il parametro interval (definito in secondi)
     * **/
    @ParameterizedTest(name = "TC_MON_01: interval={0} non valido")
    @ValueSource(ints = {0, -1})
    @DisplayName("TC_MON_01: intervallo di polling non positivo")
    void tcMon01_invalidInterval(int interval) {
        monitor.setTopology("topo-test");
        monitor.setInterval(interval);

        assertThrows(IllegalArgumentException.class, () -> monitor.metrics(nimbusMock));
    }

    /**
     * Verifica il BVA superiore del parametro Interval, passando un valore positivo e > 0
     * **/
    @Test
    @DisplayName("TC_MON_01b: Boundary valido interval=1 con Discovery Mode")
    void tcMon01b_minimumValidInterval() throws Exception {
        monitor.setTopology("topo-test");
        monitor.setComponent(null);
        monitor.setInterval(1);
        stubDiscovery("comp-1");

        assertDoesNotThrow(() -> monitor.metrics(nimbusMock));
    }

    /**
     * Test Parametrico per verificare l'attributo Topology, passando come valori null o ""
     * **/
    @ParameterizedTest(name = "TC_MON_02: topology=[{0}]")
    @NullAndEmptySource
    @DisplayName("TC_MON_02: Nome della topologia nullo o vuoto")
    void tcMon02_invalidTopology(String topology) {
        monitor.setTopology(topology);
        monitor.setInterval(4);

        assertThrows(IllegalArgumentException.class, () -> monitor.metrics(nimbusMock));
    }

    /** Test per verificare il caso component-id = null:
     * topology-name --> "topo-test"
     * interval-secs --> 4 sec.
     * component-id  --> null
     * stream-id     --> "default"
     * watch         --> "emitted" (defualt)
     * **/
    @ParameterizedTest(name = "TC_MON_03: component=[{0}]")
    @NullAndEmptySource
    @DisplayName("TC_MON_03: Nome della topologia nullo o vuoto")
    void tcMon03_componentNullDiscovery(String component) throws Exception {
        monitor.setTopology("topo-test");
        monitor.setComponent(component);
        monitor.setInterval(4);
        stubDiscovery("discovered-component-1");

        assertDoesNotThrow(() -> monitor.metrics(nimbusMock));
    }

    /**
     * Testiamo il caso in cui abbiamo un componente valido (Stringa) ma non corretto per il sistema, cioè è inesistente
     * **/
    @Test
    @DisplayName("TC_MON_03b: componente valido e non corretta")
    void tcMon03b_componentNullDiscovery() throws Exception {
        configureMonitor("comp-1","default","emitted");

        stubDiscovery("discovered-component-1");

        assertThrows(IllegalArgumentException.class, () -> monitor.metrics(nimbusMock));
    }

    /** Test per verificare stream-id = null o "":
     * topology-name --> "topo-test"
     * interval-secs --> 4 sec.
     * component-id  --> comp-1
     * stream-id     --> null/""
     * "emitted"     --> valore defalut
     * **/
    @ParameterizedTest(name = "TC_MON_04: stream=[{0}]")
    @NullAndEmptySource
    @DisplayName("TC_MON_04: stream nullo o vuoto")
    void tcMon04_invalidStream(String stream) {
        monitor.setTopology("topo-test");
        monitor.setComponent("comp-1");
        monitor.setStream(stream);
        monitor.setInterval(4);

        assertThrows(IllegalArgumentException.class, () -> monitor.metrics(nimbusMock));
    }

    /** Testa i valori null/"" associati ad una stringa non supportata, nel contratto watch ("emitted", "transfered"):
     * topology-name --> "topo-test"
     * interval-secs --> 4 sec.
     * component-id  --> comp-1
     * stream-id     --> default
     * -w                --> "invalid-metric-type"
     * **/
    @ParameterizedTest(name = "TC_MON_05: watch=[{0}]")
    @NullAndEmptySource
    @ValueSource(strings = {"invalid-metric-type"})
    @DisplayName("TC_MON_05: watch item non supportato")
    void tcMon05_invalidWatchItem(String watch) {
        monitor.setTopology("topo-test");
        monitor.setComponent("comp-1");
        monitor.setStream("default");
        monitor.setInterval(4);
        monitor.setWatch(watch);

        assertThrows(IllegalArgumentException.class, () -> monitor.metrics(nimbusMock));
    }

    // ---------------------------------------------------------------------
    // Flusso e metriche
    // ---------------------------------------------------------------------

    @ParameterizedTest(name = "TC_MON_06: watch={0}")
    @ValueSource(strings = {"emitted", "transferred"})
    @DisplayName("TC_MON_06: componente inesistente nel cluster")
    @Timeout(10)
    void tcMon06_componentNotFound(String watch) throws Exception {
        configureMonitor("missing-component", "default", watch);

        //  Creiamo un oggetto "fittizio" di tipo TopologyInfo, essendo una classe generata da Thirft
        TopologyInfo topoInfo = Mockito.mock(TopologyInfo.class);
        //  Quando chiamo il metodo 'topoInfo.get_executors()' --> ritorna una lista vuota. Simula il caso in cui la topologia non ha alcun Executor
        when(topoInfo.get_executors()).thenReturn(Collections.emptyList());
        //  Quando eseguirà la chiamata RPC passando la stringa "topo-test" e qualsiasi opzione --> ritorna il nostro oggetto topoInfo, prepatato in precedenza
        when(nimbusMock.getTopologyInfoByNameWithOpts(eq("topo-test"), any())).thenReturn(topoInfo);

        assertThrows(IllegalArgumentException.class, () -> monitor.metrics(nimbusMock));
    }

    @ParameterizedTest(name = "TC_MON_07: watch={0}")
    @ValueSource(strings = {"emitted", "transferred"})
    @DisplayName("TC_MON_07: stream richiesto assente tra quelli del componente")
    @Timeout(10)
    void tcMon07_streamNotFound(String watch) throws Exception {
        configureMonitor("comp-1", "missing-stream", watch);

        Map<String, Map<String, Long>> metrics = new HashMap<>();
        metrics.put(WINDOW, new HashMap<>(Map.of("other", 1L)));
        stubExecutor("comp-1", watch, metrics);

        assertThrows(IllegalArgumentException.class, () -> monitor.metrics(nimbusMock));
    }

    // ---------------------------------------------------------------------
    // Helper per la configurazione dei mock
    // ---------------------------------------------------------------------

    /**
     * Metodo per la configurazione di un input valido da CLI:
     * storm monitor topo-test -i 4 -m 'component' -s 'stream' - w 'watch'
     * **/
    private void configureMonitor(String component, String stream, String watch) {
        monitor.setTopology("topo-test");
        monitor.setComponent(component);
        monitor.setStream(stream);
        monitor.setWatch(watch);
        monitor.setInterval(4);
    }

    /**
     * Metodo per replicare lo scenario della 'Discovery Mode', quando l'utente non specifica il componente
     * **/
    private void stubDiscovery(String componentId) throws Exception {

        TopologyInfo topoInfo = Mockito.mock(TopologyInfo.class);
        ExecutorSummary exec = Mockito.mock(ExecutorSummary.class);

        when(exec.get_component_id()).thenReturn(componentId);
        //  Quando verrà chiesta la lista di tutti gli Executor presenti, ritorna una lista immutabile che contiene esclusivamente un solo executor
        when(topoInfo.get_executors()).thenReturn(Collections.singletonList(exec));
        //  Simuliamo il ritorno della chiamata RPC al servizio esterno Nimbus
        when(nimbusMock.getTopologyInfoByNameWithOpts(eq("topo-test"), any())).thenReturn(topoInfo);
    }

    /**
     * Metodo per simulare che i componenti abbiano generato dei dati.
     * Paramtri in Input:
     * componentId  : String    --> ID del componente che vogliamo simulare
     * watch        : String    --> Metrica che vogliamo monitorare ("emitted"/"transfered")
     * metrics      : Map       --> Struttura dati con i numeri effettivi, che vogliamo far restituire al cluster
     * **/
    private void stubExecutor(String componentId, String watch,
                              Map<String, Map<String, Long>> metrics) throws Exception {
        TopologyInfo topoInfo = Mockito.mock(TopologyInfo.class);
        ExecutorSummary exec = Mockito.mock(ExecutorSummary.class);
        ExecutorStats stats = Mockito.mock(ExecutorStats.class);

        when(exec.get_component_id()).thenReturn(componentId);
        when(exec.get_stats()).thenReturn(stats);
        if ("emitted".equals(watch)) {
            when(stats.get_emitted()).thenReturn(metrics);
        } else {
            when(stats.get_transferred()).thenReturn(metrics);
        }
        when(topoInfo.get_executors()).thenReturn(Collections.singletonList(exec));
        when(nimbusMock.getTopologyInfoByNameWithOpts(eq("topo-test"), any())).thenReturn(topoInfo);
    }
}
