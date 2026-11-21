package org.apache.storm.utils.staticmocking;

import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.utils.Monitor;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;


public class MonitorMTTest {

    private Monitor monitor;
    private Nimbus.Iface nimbusMock;

    @BeforeEach
    void setUp() {
        monitor = new Monitor();
        nimbusMock = Mockito.mock(Nimbus.Iface.class);
    }

    @Test
    void testMetricsNullStateThrowsException() throws Exception {
        // 1. Configuriamo i parametri validi del monitor (topology, component, stream, watch, interval)
        configureMonitor("my-component", "my-stream", "emitted");

        // 2. Prepariamo la struttura dati dei metric map attesa dai mock
        // Questo serve a superare i controlli di validazione ed evitare IllegalArgumentException prematuri
        Map<String, Long> allTimeMap = new HashMap<>();
        allTimeMap.put("my-stream", 150L); // Valore di stream fittizio

        Map<String, Map<String, Long>> metricsMap = new HashMap<>();
        metricsMap.put(":all-time", allTimeMap);

        // 3. Sfruttiamo il tuo helper per stubbare il comportamento di Nimbus e degli ExecutorStats
        // In questo modo componentParallelism > 0 e streamFound = true
        stubExecutor("my-component", "emitted", metricsMap);

        // 4. Esecuzione e Asserzione:
        // Passando 'state = null', il codice arriverà alla riga: "long timeDelta = now - state.getLastTime();"
        // e scatterà inevitabilmente il NullPointerException.
        long now = System.currentTimeMillis();

        assertThrows(NullPointerException.class, () -> monitor.metrics(nimbusMock, now, null), "Ci si aspettava un NullPointerException passando uno state nullo");
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
