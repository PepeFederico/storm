package org.apache.storm.utils.staticmocking;

import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.utils.Monitor;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
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
 * Suite White-Box per {@link Monitor}.
 * Fase 2: Garanzie di copertura di tutta la classe, attraverso l'analisi del codice effettivo.
 * I mock di Nimbus controllano solo cio' che i collaboratori restituiscono.
 */
@Tag("white-box")
public class MonitorWBTest {
    // Chiave della finestra temporale nelle statistiche Thrift (solo setup dei mock)
    private static final String WINDOW = ":all-time";

    private Monitor monitor;
    private Nimbus.Iface nimbusMock;

    @BeforeEach
    void setUp() {
        monitor = new Monitor();
        nimbusMock = Mockito.mock(Nimbus.Iface.class);
    }

    /**
     * Abbiamo bisogno di testare il caso in cui, il sistema esegue il campionamento delle metriche e produce in output il valore associato.
     * Come detto, dal report JaCoCo e dalla possibilità di osservare il come è stato implementato il codice, dobbiamo necessariamente testare il ciclo do-while(true)
     * in un ambiente sicuro e controllato, per evitare iterazione indefinite sul suddetto frame di codice.
     */
    @ParameterizedTest(name = "TC_MON_WB_08: watch={0}")
    @ValueSource(strings = {"emitted", "transferred"})
    @DisplayName("TC_MON_WB_08: Caso d'uso defualt via API pubblica metrics(client)")
    void tcMon08_wb(String watch) throws Exception {
        configureMonitor("comp-1", "default", watch);
        monitor.setInterval(1);

        Map<String, Map<String, Long>> metrics = new HashMap<>();
        //  con WINDOW definiamo la finestra temporale, 1° livello esterno, mentre 2° livello è il nome dello stream, ed il numero di messaggi
        metrics.put(WINDOW, new HashMap<>(Map.of("default", 5000L)));
        stubExecutor("comp-1", watch, metrics);

        // metrics(client) cicla all'infinito: lo eseguo in un thread e lo interrompo
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread poller = new Thread(() -> {
            try {
                monitor.metrics(nimbusMock);
            } catch (Throwable t) {
                thrown.set(t);
            }
        });
        poller.start();

        try {
            // la prima iterazione di campionamento ha interrogato Nimbus
            verify(nimbusMock, timeout(4000).atLeast(2))
                    .getTopologyInfoByNameWithOpts(eq("topo-test"), any());
        } finally {
            poller.interrupt();
            poller.join(5000);
        }

        assertFalse(poller.isAlive(), "Il thread di polling deve terminare dopo l'interrupt.");
        // Unica terminazione ammessa: l'interruzione. Qualsiasi altra eccezione indica un fallimento dell'iterazione.
        assertTrue(thrown.get() == null || thrown.get() instanceof InterruptedException,
                "Terminazione inattesa: " + thrown.get());
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
