package org.apache.storm.utils.staticmocking;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.utils.Monitor;
import org.mockito.Mockito;

/**
 * Factory pubbliche passate a Randoop con --testclass: Randoop le usa come
 * produttori di oggetti (Monitor e Nimbus.Iface) per costruire le sequenze.
 * Ogni factory restituisce uno "scenario" di Nimbus con dati coerenti, cosi'
 * Randoop non deve indovinare strutture Thrift valide.
 *
 * Nessuno scenario raggiunge il ciclo di polling di Monitor.metrics(client):
 * il percorso con metriche complete e' lasciato alle suite manuali (BB, WB).
 * Una versione precedente lo raggiungeva interrompendo il thread dal mock, ma lo
 * stato di interruzione, se lasciato attivo, disturbava Randoop (quasi tutte le
 * sequenze risultavano non compilabili).
 */
public class RandoopMonitorHelper {

    private static final String TOPOLOGY = "topo-test";
    private static final String COMPONENT = "comp-1";
    private static final String STREAM = "default";

    /** Classe di utilita': impedisce a Randoop di istanziarla e testarla. */
    private RandoopMonitorHelper() {
    }

    /** Monitor gia' configurato con valori validi, coerenti con gli scenari di Nimbus. */
    public static Monitor createMonitor() {
        Monitor monitor = new Monitor();
        monitor.setTopology(TOPOLOGY);
        monitor.setComponent(COMPONENT);
        monitor.setStream(STREAM);
        monitor.setWatch("emitted");
        monitor.setInterval(1);
        return monitor;
    }

    /** Nimbus senza executor. */
    public static Nimbus.Iface createMockNimbus() {
        return mockNimbus(new ArrayList<>());
    }

    /** Nimbus con executor di componenti diversi da quello monitorato, senza stats. */
    public static Nimbus.Iface createMockNimbusOtherComponents() {
        return mockNimbus(Arrays.asList(executor("other-1", null), executor("other-2", null)));
    }

    /**
     * Nimbus con tre executor del componente monitorato, ciascuno con metriche incomplete:
     * stats nulle, mappe delle metriche nulle, finestra :all-time assente.
     */
    public static Nimbus.Iface createMockNimbusIncompleteMetrics() {
        ExecutorStats nullMaps = Mockito.mock(ExecutorStats.class);
        Mockito.when(nullMaps.get_emitted()).thenReturn(null);
        Mockito.when(nullMaps.get_transferred()).thenReturn(null);

        ExecutorStats noWindow = Mockito.mock(ExecutorStats.class);
        Mockito.when(noWindow.get_emitted()).thenReturn(new HashMap<>());
        Mockito.when(noWindow.get_transferred()).thenReturn(new HashMap<>());

        return mockNimbus(Arrays.asList(
                executor(COMPONENT, null),
                executor(COMPONENT, nullMaps),
                executor(COMPONENT, noWindow)));
    }

    // ---------------------------------------------------------------------
    // Metodi privati: non vengono usati da Randoop
    // ---------------------------------------------------------------------

    private static Nimbus.Iface mockNimbus(List<ExecutorSummary> executors) {
        try {
            TopologyInfo info = Mockito.mock(TopologyInfo.class);
            Mockito.when(info.get_executors()).thenReturn(executors);

            Nimbus.Iface nimbus = Mockito.mock(Nimbus.Iface.class);
            Mockito.when(nimbus.getTopologyInfoByNameWithOpts(Mockito.anyString(), Mockito.any()))
                    .thenReturn(info);
            return nimbus;
        } catch (Exception e) {
            // getTopologyInfoByNameWithOpts dichiara eccezioni Thrift controllate
            throw new IllegalStateException("Impossibile configurare il mock di Nimbus", e);
        }
    }

    private static ExecutorSummary executor(String componentId, ExecutorStats stats) {
        ExecutorSummary executor = Mockito.mock(ExecutorSummary.class);
        Mockito.when(executor.get_component_id()).thenReturn(componentId);
        Mockito.when(executor.get_stats()).thenReturn(stats);
        return executor;
    }
}