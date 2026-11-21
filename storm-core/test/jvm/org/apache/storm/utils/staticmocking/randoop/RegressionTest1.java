package org.apache.storm.utils.staticmocking.randoop;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RegressionTest1 {

    public static boolean debug = false;

    public void assertBooleanArrayEquals(boolean[] expectedArray, boolean[] actualArray) {
        if (expectedArray.length != actualArray.length) {
            throw new AssertionError("Array lengths differ: " + expectedArray.length + " != " + actualArray.length);
        }
        for (int i = 0; i < expectedArray.length; i++) {
            if (expectedArray[i] != actualArray[i]) {
                throw new AssertionError("Arrays differ at index " + i + ": " + expectedArray[i] + " != " + actualArray[i]);
            }
        }
    }

    @Test
    public void test0501() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0501");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        org.apache.storm.utils.Monitor monitor9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor9.setInterval((int) (short) -1);
        monitor9.setInterval((int) 'a');
        monitor9.setStream("");
        monitor9.setWatch("");
        monitor9.setInterval((int) (byte) 1);
        monitor9.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface22 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor9.metrics(iface22);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface22);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor9);
        org.junit.Assert.assertNotNull(iface22);
    }

    @Test
    public void test0502() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0502");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0503() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0503");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0504() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0504");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setInterval((int) (byte) 0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0505() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0505");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor9.setTopology("");
        monitor9.setComponent("hi!");
        monitor9.setStream("");
        monitor9.setComponent("");
        monitor9.setTopology("");
        monitor9.setStream("");
        monitor9.setWatch("hi!");
        monitor9.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface26 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor9.metrics(iface26);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface26);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor9);
        org.junit.Assert.assertNotNull(iface26);
    }

    @Test
    public void test0506() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0506");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor9.setInterval((int) (short) -1);
        monitor9.setInterval((int) 'a');
        monitor9.setStream("");
        monitor9.setWatch("");
        monitor9.setInterval((int) (byte) 1);
        monitor9.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface22 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor9.metrics(iface22);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface22);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: comp-1 not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor9);
        org.junit.Assert.assertNotNull(iface22);
    }

    @Test
    public void test0507() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0507");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0508() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0508");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0509() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0509");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        org.apache.storm.utils.Monitor monitor19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor19.setInterval((int) (short) -1);
        monitor19.setInterval((int) 'a');
        monitor19.setStream("");
        monitor19.setWatch("hi!");
        monitor19.setInterval((int) (short) 0);
        monitor19.setComponent("");
        monitor19.setStream("hi!");
        monitor19.setInterval((int) (byte) 1);
        org.apache.storm.utils.Monitor monitor36 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor36.setInterval((int) (short) -1);
        monitor36.setInterval((int) 'a');
        monitor36.setInterval((int) ' ');
        monitor36.setComponent("");
        monitor36.setTopology("hi!");
        monitor36.setInterval(10);
        org.apache.storm.utils.Monitor monitor49 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor49.setInterval((int) (short) -1);
        monitor49.setInterval((int) 'a');
        monitor49.setInterval((int) ' ');
        monitor49.setComponent("");
        monitor49.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface60 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor49.metrics(iface60);
        monitor36.metrics(iface60);
        monitor19.metrics(iface60);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface60);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor19);
        org.junit.Assert.assertNotNull(monitor36);
        org.junit.Assert.assertNotNull(monitor49);
        org.junit.Assert.assertNotNull(iface60);
    }

    @Test
    public void test0510() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0510");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0511() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0511");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0512() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0512");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor0.metrics(iface11);
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0513() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0513");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0514() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0514");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setInterval(10);
        monitor0.setWatch("");
        monitor0.setComponent("");
        monitor0.setInterval(100);
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0515() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0515");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0516() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0516");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0517() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0517");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0518() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0518");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0519() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0519");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setInterval((int) '4');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass25 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass25);
    }

    @Test
    public void test0520() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0520");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0521() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0521");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0522() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0522");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0523() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0523");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 1);
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0524() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0524");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setStream("");
        monitor0.setInterval(1);
        monitor0.setStream("");
        monitor0.setInterval((-1));
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0525() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0525");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0526() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0526");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setInterval(0);
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0527() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0527");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0528() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0528");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) 100);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0529() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0529");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
    }

    @Test
    public void test0530() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0530");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0531() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0531");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0532() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0532");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0533() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0533");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0534() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0534");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0535() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0535");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setInterval(0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0536() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0536");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0537() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0537");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0538() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0538");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0539() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0539");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0540() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0540");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor21 = new org.apache.storm.utils.Monitor();
        monitor21.setStream("hi!");
        monitor21.setTopology("hi!");
        monitor21.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface28 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor21.metrics(iface28);
        monitor21.setComponent("");
        org.apache.storm.utils.Monitor monitor32 = new org.apache.storm.utils.Monitor();
        monitor32.setStream("hi!");
        monitor32.setTopology("hi!");
        monitor32.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface39 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor32.metrics(iface39);
        monitor21.metrics(iface39);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface39);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface28);
        org.junit.Assert.assertNotNull(iface39);
    }

    @Test
    public void test0541() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0541");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0542() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0542");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setStream("");
        org.apache.storm.utils.Monitor monitor11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor11.setInterval((int) (short) -1);
        monitor11.setInterval((int) 'a');
        monitor11.setStream("");
        monitor11.setWatch("hi!");
        monitor11.setInterval((int) (short) 0);
        monitor11.setComponent("");
        monitor11.setStream("hi!");
        monitor11.setInterval((int) (byte) 1);
        org.apache.storm.utils.Monitor monitor28 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor28.setInterval((int) (short) -1);
        monitor28.setInterval((int) 'a');
        monitor28.setInterval((int) ' ');
        monitor28.setComponent("");
        monitor28.setTopology("hi!");
        monitor28.setInterval(10);
        org.apache.storm.utils.Monitor monitor41 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor41.setInterval((int) (short) -1);
        monitor41.setInterval((int) 'a');
        monitor41.setInterval((int) ' ');
        monitor41.setComponent("");
        monitor41.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface52 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor41.metrics(iface52);
        monitor28.metrics(iface52);
        monitor11.metrics(iface52);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface52);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(monitor28);
        org.junit.Assert.assertNotNull(monitor41);
        org.junit.Assert.assertNotNull(iface52);
    }

    @Test
    public void test0543() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0543");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setInterval((int) '4');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval(0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0544() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0544");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (short) 100);
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) -1);
    }

    @Test
    public void test0545() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0545");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        org.apache.storm.utils.Monitor monitor11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor11.setTopology("hi!");
        monitor11.setComponent("");
        monitor11.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor18 = new org.apache.storm.utils.Monitor();
        monitor18.setStream("hi!");
        monitor18.setTopology("hi!");
        monitor18.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface25 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor18.metrics(iface25);
        monitor11.metrics(iface25);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface25);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(iface25);
    }

    @Test
    public void test0546() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0546");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor11.setInterval((int) (short) -1);
        monitor11.setInterval((int) 'a');
        monitor11.setInterval((int) ' ');
        monitor11.setComponent("");
        monitor11.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface22 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor11.metrics(iface22);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface22);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(iface22);
    }

    @Test
    public void test0547() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0547");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0548() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0548");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0549() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0549");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor0.metrics(iface11);
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (byte) 0);
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0550() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0550");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("hi!");
        org.apache.storm.generated.Nimbus.Iface iface19 = null;
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface19);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0551() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0551");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval(1);
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0552() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0552");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setStream("");
        monitor0.setInterval(10);
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0553() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0553");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setInterval((int) (byte) 0);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0554() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0554");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("hi!");
        monitor0.setInterval(0);
        monitor0.setInterval(100);
        monitor0.setStream("");
        org.apache.storm.generated.Nimbus.Iface iface13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface13);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface13);
    }

    @Test
    public void test0555() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0555");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0556() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0556");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (byte) 1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0557() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0557");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0558() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0558");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("");
        monitor0.setInterval(0);
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0559() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0559");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0560() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0560");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0561() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0561");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((int) (short) 0);
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) ' ');
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setTopology("hi!");
        monitor15.setComponent("");
        monitor15.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor22 = new org.apache.storm.utils.Monitor();
        monitor22.setStream("hi!");
        monitor22.setTopology("hi!");
        monitor22.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface29 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor22.metrics(iface29);
        monitor15.metrics(iface29);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface29);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream: default not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(iface29);
    }

    @Test
    public void test0562() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0562");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0563() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0563");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0564() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0564");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0565() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0565");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0566() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0566");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setStream("");
        monitor0.setInterval(10);
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass23 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass23);
    }

    @Test
    public void test0567() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0567");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0568() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0568");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0569() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0569");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0570() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0570");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface15);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface15);
    }

    @Test
    public void test0571() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0571");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 100);
        org.apache.storm.utils.Monitor monitor19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor19.setInterval((int) (short) -1);
        monitor19.setInterval((int) 'a');
        monitor19.setInterval((int) ' ');
        monitor19.setStream("");
        monitor19.setWatch("hi!");
        monitor19.setComponent("");
        monitor19.setWatch("");
        monitor19.setComponent("");
        org.apache.storm.utils.Monitor monitor36 = new org.apache.storm.utils.Monitor();
        monitor36.setStream("hi!");
        monitor36.setTopology("hi!");
        monitor36.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface43 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor36.metrics(iface43);
        monitor19.metrics(iface43);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface43);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor19);
        org.junit.Assert.assertNotNull(iface43);
    }

    @Test
    public void test0572() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0572");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor0.metrics(iface11);
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0573() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0573");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0574() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0574");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
    }

    @Test
    public void test0575() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0575");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0576() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0576");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) 'a');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0577() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0577");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0578() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0578");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0579() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0579");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0580() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0580");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 1);
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0581() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0581");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setTopology("");
        monitor15.setComponent("hi!");
        monitor15.setStream("");
        monitor15.setComponent("");
        monitor15.setTopology("");
        monitor15.setStream("");
        monitor15.setWatch("hi!");
        monitor15.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface32 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor15.metrics(iface32);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface32);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(iface32);
    }

    @Test
    public void test0582() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0582");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor0.metrics(iface7);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(iface7);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0583() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0583");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setInterval((int) '4');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0584() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0584");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        org.apache.storm.generated.Nimbus.Iface iface17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface17);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface17);
    }

    @Test
    public void test0585() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0585");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0586() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0586");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0587() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0587");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0588() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0588");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0589() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0589");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0590() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0590");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0591() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0591");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0592() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0592");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setComponent("hi!");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setInterval((int) ' ');
        monitor13.setComponent("");
        monitor13.setTopology("hi!");
        monitor13.setInterval(10);
        org.apache.storm.utils.Monitor monitor26 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor26.setInterval((int) (short) -1);
        monitor26.setInterval((int) 'a');
        monitor26.setInterval((int) ' ');
        monitor26.setComponent("");
        monitor26.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface37 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor26.metrics(iface37);
        monitor13.metrics(iface37);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface37);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(monitor26);
        org.junit.Assert.assertNotNull(iface37);
    }

    @Test
    public void test0593() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0593");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass21 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test0594() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0594");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0595() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0595");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setTopology("");
        monitor0.setInterval((int) '#');
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0596() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0596");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setInterval(10);
        monitor0.setStream("hi!");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setInterval((int) ' ');
        monitor15.setComponent("");
        monitor15.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface26 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor15.metrics(iface26);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface26);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: comp-1 not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(iface26);
    }

    @Test
    public void test0597() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0597");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setStream("");
        monitor15.setWatch("hi!");
        monitor15.setInterval((int) (short) 0);
        monitor15.setComponent("");
        monitor15.setStream("hi!");
        monitor15.setInterval((int) (byte) 1);
        org.apache.storm.utils.Monitor monitor32 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor32.setInterval((int) (short) -1);
        monitor32.setInterval((int) 'a');
        monitor32.setInterval((int) ' ');
        monitor32.setComponent("");
        monitor32.setTopology("hi!");
        monitor32.setInterval(10);
        org.apache.storm.utils.Monitor monitor45 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor45.setInterval((int) (short) -1);
        monitor45.setInterval((int) 'a');
        monitor45.setInterval((int) ' ');
        monitor45.setComponent("");
        monitor45.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface56 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor45.metrics(iface56);
        monitor32.metrics(iface56);
        monitor15.metrics(iface56);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface56);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(monitor32);
        org.junit.Assert.assertNotNull(monitor45);
        org.junit.Assert.assertNotNull(iface56);
    }

    @Test
    public void test0598() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0598");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        org.apache.storm.utils.Monitor monitor11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor11.setInterval((int) (short) -1);
        monitor11.setInterval((int) 'a');
        monitor11.setInterval((int) ' ');
        monitor11.setStream("");
        monitor11.setWatch("hi!");
        monitor11.setComponent("");
        monitor11.setStream("hi!");
        org.apache.storm.utils.Monitor monitor26 = new org.apache.storm.utils.Monitor();
        monitor26.setStream("hi!");
        monitor26.setTopology("hi!");
        monitor26.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface33 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor26.metrics(iface33);
        monitor26.setComponent("");
        org.apache.storm.utils.Monitor monitor37 = new org.apache.storm.utils.Monitor();
        monitor37.setStream("hi!");
        monitor37.setTopology("hi!");
        monitor37.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface44 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor37.metrics(iface44);
        monitor26.metrics(iface44);
        monitor11.metrics(iface44);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface44);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(iface33);
        org.junit.Assert.assertNotNull(iface44);
    }

    @Test
    public void test0599() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0599");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setInterval((int) '4');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass25 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass25);
    }

    @Test
    public void test0600() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0600");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setStream("");
        monitor0.setComponent("");
        org.apache.storm.utils.Monitor monitor19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor19.setInterval((int) (short) -1);
        monitor19.setInterval((int) 'a');
        monitor19.setInterval((int) ' ');
        monitor19.setComponent("");
        monitor19.setTopology("hi!");
        monitor19.setComponent("hi!");
        monitor19.setComponent("");
        monitor19.setStream("");
        monitor19.setComponent("");
        monitor19.setWatch("");
        org.apache.storm.utils.Monitor monitor40 = new org.apache.storm.utils.Monitor();
        monitor40.setStream("hi!");
        monitor40.setTopology("hi!");
        monitor40.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface47 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor40.metrics(iface47);
        monitor40.setComponent("");
        org.apache.storm.utils.Monitor monitor51 = new org.apache.storm.utils.Monitor();
        monitor51.setStream("hi!");
        monitor51.setTopology("hi!");
        monitor51.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface58 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor51.metrics(iface58);
        monitor40.metrics(iface58);
        monitor19.metrics(iface58);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface58);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor19);
        org.junit.Assert.assertNotNull(iface47);
        org.junit.Assert.assertNotNull(iface58);
    }

    @Test
    public void test0601() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0601");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0602() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0602");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0603() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0603");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0604() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0604");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor11.setInterval((int) (short) -1);
        monitor11.setInterval((int) 'a');
        monitor11.setInterval((int) ' ');
        monitor11.setStream("");
        monitor11.setWatch("hi!");
        monitor11.setComponent("");
        monitor11.setStream("hi!");
        org.apache.storm.utils.Monitor monitor26 = new org.apache.storm.utils.Monitor();
        monitor26.setStream("hi!");
        monitor26.setTopology("hi!");
        monitor26.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface33 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor26.metrics(iface33);
        monitor26.setComponent("");
        org.apache.storm.utils.Monitor monitor37 = new org.apache.storm.utils.Monitor();
        monitor37.setStream("hi!");
        monitor37.setTopology("hi!");
        monitor37.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface44 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor37.metrics(iface44);
        monitor26.metrics(iface44);
        monitor11.metrics(iface44);
        org.apache.storm.utils.Monitor monitor48 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor48.setInterval((int) (short) -1);
        monitor48.setInterval((int) 'a');
        monitor48.setInterval((int) ' ');
        monitor48.setComponent("");
        monitor48.setTopology("hi!");
        monitor48.setInterval(10);
        org.apache.storm.utils.Monitor monitor61 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor61.setInterval((int) (short) -1);
        monitor61.setInterval((int) 'a');
        monitor61.setInterval((int) ' ');
        monitor61.setComponent("");
        monitor61.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface72 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor61.metrics(iface72);
        monitor48.metrics(iface72);
        monitor11.metrics(iface72);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface72);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(iface33);
        org.junit.Assert.assertNotNull(iface44);
        org.junit.Assert.assertNotNull(monitor48);
        org.junit.Assert.assertNotNull(monitor61);
        org.junit.Assert.assertNotNull(iface72);
    }

    @Test
    public void test0605() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0605");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0606() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0606");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) 10);
        org.apache.storm.generated.Nimbus.Iface iface15 = null;
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface15);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0607() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0607");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) -1);
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0608() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0608");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0609() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0609");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("hi!");
        monitor0.setInterval(0);
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0610() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0610");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 1);
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0611() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0611");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setInterval((int) '4');
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setInterval((int) '4');
    }

    @Test
    public void test0612() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0612");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0613() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0613");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0614() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0614");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setInterval(10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0615() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0615");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor0.metrics(iface11);
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0616() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0616");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor0.metrics(iface11);
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0617() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0617");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setStream("");
        monitor0.setInterval(10);
        monitor0.setComponent("");
        monitor0.setInterval((int) (byte) 10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0618() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0618");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0619() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0619");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setInterval(10);
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0620() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0620");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0621() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0621");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setInterval((int) ' ');
        monitor15.setStream("");
        monitor15.setWatch("hi!");
        monitor15.setComponent("");
        monitor15.setStream("hi!");
        org.apache.storm.utils.Monitor monitor30 = new org.apache.storm.utils.Monitor();
        monitor30.setStream("hi!");
        monitor30.setTopology("hi!");
        monitor30.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface37 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor30.metrics(iface37);
        monitor30.setComponent("");
        org.apache.storm.utils.Monitor monitor41 = new org.apache.storm.utils.Monitor();
        monitor41.setStream("hi!");
        monitor41.setTopology("hi!");
        monitor41.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface48 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor41.metrics(iface48);
        monitor30.metrics(iface48);
        monitor15.metrics(iface48);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface48);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(iface37);
        org.junit.Assert.assertNotNull(iface48);
    }

    @Test
    public void test0622() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0622");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0623() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0623");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setInterval((-1));
        monitor0.setStream("hi!");
        org.apache.storm.utils.Monitor monitor9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor9.setInterval((int) (short) -1);
        monitor9.setInterval((int) 'a');
        monitor9.setInterval((int) ' ');
        monitor9.setStream("");
        monitor9.setWatch("hi!");
        monitor9.setComponent("");
        monitor9.setWatch("");
        monitor9.setComponent("");
        org.apache.storm.utils.Monitor monitor26 = new org.apache.storm.utils.Monitor();
        monitor26.setStream("hi!");
        monitor26.setTopology("hi!");
        monitor26.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface33 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor26.metrics(iface33);
        monitor9.metrics(iface33);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface33);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor9);
        org.junit.Assert.assertNotNull(iface33);
    }

    @Test
    public void test0624() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0624");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0625() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0625");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setTopology("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0626() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0626");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0627() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0627");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setInterval(1);
        java.lang.Class<?> wildcardClass21 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test0628() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0628");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        monitor0.setInterval((-1));
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0629() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0629");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setInterval((int) (byte) -1);
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0630() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0630");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (short) 0);
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0631() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0631");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass5 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0632() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0632");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0633() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0633");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        org.apache.storm.generated.Nimbus.Iface iface23 = null;
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface23);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.generated.Nimbus$Iface.getTopologyInfoByNameWithOpts(String, org.apache.storm.generated.GetInfoOptions)\" because \"client\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0634() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0634");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setInterval((-1));
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0635() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0635");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setInterval(10);
        monitor0.setStream("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0636() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0636");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0637() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0637");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0638() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0638");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setStream("");
        monitor13.setWatch("");
        monitor13.setInterval((int) (byte) 1);
        monitor13.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface26 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor13.metrics(iface26);
        org.apache.storm.utils.Monitor monitor28 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor28.setInterval((int) (short) -1);
        monitor28.setInterval((int) 'a');
        monitor28.setInterval((int) ' ');
        monitor28.setComponent("");
        monitor28.setTopology("hi!");
        monitor28.setComponent("hi!");
        monitor28.setComponent("");
        monitor28.setStream("");
        monitor28.setComponent("");
        monitor28.setWatch("");
        org.apache.storm.utils.Monitor monitor49 = new org.apache.storm.utils.Monitor();
        monitor49.setStream("hi!");
        monitor49.setTopology("hi!");
        monitor49.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface56 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor49.metrics(iface56);
        monitor49.setComponent("");
        org.apache.storm.utils.Monitor monitor60 = new org.apache.storm.utils.Monitor();
        monitor60.setStream("hi!");
        monitor60.setTopology("hi!");
        monitor60.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface67 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor60.metrics(iface67);
        monitor49.metrics(iface67);
        monitor28.metrics(iface67);
        monitor13.metrics(iface67);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface67);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface26);
        org.junit.Assert.assertNotNull(monitor28);
        org.junit.Assert.assertNotNull(iface56);
        org.junit.Assert.assertNotNull(iface67);
    }

    @Test
    public void test0639() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0639");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0640() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0640");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 1);
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: hi! not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0641() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0641");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setInterval((int) '4');
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setStream("hi!");
    }

    @Test
    public void test0642() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0642");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0643() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0643");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0644() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0644");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0645() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0645");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0646() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0646");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        org.apache.storm.utils.Monitor monitor11 = new org.apache.storm.utils.Monitor();
        monitor11.setStream("hi!");
        monitor11.setTopology("hi!");
        monitor11.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface18 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor11.metrics(iface18);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface18);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface18);
    }

    @Test
    public void test0647() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0647");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setStream("");
        monitor0.setStream("hi!");
    }

    @Test
    public void test0648() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0648");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setInterval(10);
        monitor0.setWatch("");
        monitor0.setComponent("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0649() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0649");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor0.metrics(iface11);
        monitor0.setComponent("hi!");
        org.apache.storm.utils.Monitor monitor15 = new org.apache.storm.utils.Monitor();
        monitor15.setStream("hi!");
        monitor15.setTopology("hi!");
        monitor15.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface22 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor15.metrics(iface22);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface22);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: hi! not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
        org.junit.Assert.assertNotNull(iface22);
    }

    @Test
    public void test0650() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0650");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0651() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0651");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0652() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0652");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.apache.storm.utils.Monitor monitor15 = new org.apache.storm.utils.Monitor();
        monitor15.setStream("hi!");
        monitor15.setTopology("hi!");
        monitor15.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface22 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor15.metrics(iface22);
        monitor15.setComponent("");
        org.apache.storm.utils.Monitor monitor26 = new org.apache.storm.utils.Monitor();
        monitor26.setStream("hi!");
        monitor26.setTopology("hi!");
        monitor26.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface33 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor26.metrics(iface33);
        monitor15.metrics(iface33);
        monitor0.metrics(iface33);
        org.apache.storm.utils.Monitor monitor37 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor37.setInterval((int) (short) -1);
        monitor37.setInterval((int) 'a');
        monitor37.setInterval((int) ' ');
        monitor37.setComponent("");
        monitor37.setTopology("hi!");
        monitor37.setInterval(10);
        org.apache.storm.utils.Monitor monitor50 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor50.setInterval((int) (short) -1);
        monitor50.setInterval((int) 'a');
        monitor50.setInterval((int) ' ');
        monitor50.setComponent("");
        monitor50.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface61 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor50.metrics(iface61);
        monitor37.metrics(iface61);
        monitor0.metrics(iface61);
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface22);
        org.junit.Assert.assertNotNull(iface33);
        org.junit.Assert.assertNotNull(monitor37);
        org.junit.Assert.assertNotNull(monitor50);
        org.junit.Assert.assertNotNull(iface61);
    }

    @Test
    public void test0653() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0653");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setInterval((int) ' ');
        monitor13.setStream("");
        monitor13.setWatch("hi!");
        monitor13.setComponent("");
        monitor13.setStream("hi!");
        org.apache.storm.utils.Monitor monitor28 = new org.apache.storm.utils.Monitor();
        monitor28.setStream("hi!");
        monitor28.setTopology("hi!");
        monitor28.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface35 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor28.metrics(iface35);
        monitor28.setComponent("");
        org.apache.storm.utils.Monitor monitor39 = new org.apache.storm.utils.Monitor();
        monitor39.setStream("hi!");
        monitor39.setTopology("hi!");
        monitor39.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface46 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor39.metrics(iface46);
        monitor28.metrics(iface46);
        monitor13.metrics(iface46);
        org.apache.storm.utils.Monitor monitor50 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor50.setInterval((int) (short) -1);
        monitor50.setInterval((int) 'a');
        monitor50.setInterval((int) ' ');
        monitor50.setComponent("");
        monitor50.setTopology("hi!");
        monitor50.setInterval(10);
        org.apache.storm.utils.Monitor monitor63 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor63.setInterval((int) (short) -1);
        monitor63.setInterval((int) 'a');
        monitor63.setInterval((int) ' ');
        monitor63.setComponent("");
        monitor63.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface74 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor63.metrics(iface74);
        monitor50.metrics(iface74);
        monitor13.metrics(iface74);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface74);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface35);
        org.junit.Assert.assertNotNull(iface46);
        org.junit.Assert.assertNotNull(monitor50);
        org.junit.Assert.assertNotNull(monitor63);
        org.junit.Assert.assertNotNull(iface74);
    }

    @Test
    public void test0654() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0654");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setStream("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0655() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0655");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0656() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0656");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setStream("");
        monitor15.setWatch("hi!");
        monitor15.setInterval((int) (short) 0);
        monitor15.setComponent("");
        monitor15.setStream("hi!");
        monitor15.setInterval((int) (byte) 1);
        org.apache.storm.utils.Monitor monitor32 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor32.setInterval((int) (short) -1);
        monitor32.setInterval((int) 'a');
        monitor32.setInterval((int) ' ');
        monitor32.setComponent("");
        monitor32.setTopology("hi!");
        monitor32.setInterval(10);
        org.apache.storm.utils.Monitor monitor45 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor45.setInterval((int) (short) -1);
        monitor45.setInterval((int) 'a');
        monitor45.setInterval((int) ' ');
        monitor45.setComponent("");
        monitor45.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface56 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor45.metrics(iface56);
        monitor32.metrics(iface56);
        monitor15.metrics(iface56);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface56);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(monitor32);
        org.junit.Assert.assertNotNull(monitor45);
        org.junit.Assert.assertNotNull(iface56);
    }

    @Test
    public void test0657() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0657");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0658() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0658");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setTopology("");
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0659() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0659");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0660() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0660");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0661() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0661");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0662() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0662");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        org.apache.storm.utils.Monitor monitor9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor9.setInterval((int) (short) -1);
        monitor9.setInterval((int) 'a');
        monitor9.setInterval((int) ' ');
        monitor9.setStream("");
        monitor9.setWatch("hi!");
        monitor9.setComponent("");
        monitor9.setWatch("");
        org.apache.storm.utils.Monitor monitor24 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor24.setInterval((int) (short) -1);
        monitor24.setInterval((int) 'a');
        monitor24.setStream("");
        monitor24.setWatch("");
        monitor24.setInterval((int) (byte) 1);
        monitor24.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface37 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor24.metrics(iface37);
        monitor9.metrics(iface37);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface37);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor9);
        org.junit.Assert.assertNotNull(monitor24);
        org.junit.Assert.assertNotNull(iface37);
    }

    @Test
    public void test0663() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0663");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor0.metrics(iface11);
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0664() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0664");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0665() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0665");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) -1);
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0666() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0666");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setTopology("");
        org.apache.storm.generated.Nimbus.Iface iface11 = null;
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0667() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0667");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setInterval(10);
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0668() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0668");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0669() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0669");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (byte) -1);
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0670() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0670");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0671() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0671");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0672() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0672");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
    }

    @Test
    public void test0673() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0673");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor21 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor21.setInterval((int) (short) -1);
        monitor21.setInterval((int) 'a');
        monitor21.setStream("");
        monitor21.setWatch("hi!");
        monitor21.setInterval((int) (short) 0);
        monitor21.setComponent("");
        monitor21.setStream("hi!");
        monitor21.setInterval((int) (byte) 1);
        org.apache.storm.utils.Monitor monitor38 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor38.setInterval((int) (short) -1);
        monitor38.setInterval((int) 'a');
        monitor38.setInterval((int) ' ');
        monitor38.setComponent("");
        monitor38.setTopology("hi!");
        monitor38.setInterval(10);
        org.apache.storm.utils.Monitor monitor51 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor51.setInterval((int) (short) -1);
        monitor51.setInterval((int) 'a');
        monitor51.setInterval((int) ' ');
        monitor51.setComponent("");
        monitor51.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface62 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor51.metrics(iface62);
        monitor38.metrics(iface62);
        monitor21.metrics(iface62);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface62);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor21);
        org.junit.Assert.assertNotNull(monitor38);
        org.junit.Assert.assertNotNull(monitor51);
        org.junit.Assert.assertNotNull(iface62);
    }

    @Test
    public void test0674() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0674");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (byte) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0675() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0675");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0676() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0676");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0677() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0677");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("hi!");
        org.apache.storm.utils.Monitor monitor21 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor21.setInterval((int) (short) -1);
        monitor21.setInterval((int) 'a');
        monitor21.setStream("");
        monitor21.setWatch("");
        monitor21.setInterval((int) (byte) 1);
        monitor21.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface34 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor21.metrics(iface34);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface34);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor21);
        org.junit.Assert.assertNotNull(iface34);
    }

    @Test
    public void test0678() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0678");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) 100);
        org.apache.storm.utils.Monitor monitor7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor7.setInterval((int) (short) -1);
        monitor7.setInterval((int) 'a');
        monitor7.setStream("");
        monitor7.setWatch("");
        monitor7.setInterval((int) (byte) 1);
        monitor7.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface20 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor7.metrics(iface20);
        org.apache.storm.utils.Monitor monitor22 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor22.setInterval((int) (short) -1);
        monitor22.setInterval((int) 'a');
        monitor22.setInterval((int) ' ');
        monitor22.setComponent("");
        monitor22.setTopology("hi!");
        monitor22.setComponent("hi!");
        monitor22.setComponent("");
        monitor22.setStream("");
        monitor22.setComponent("");
        monitor22.setWatch("");
        org.apache.storm.utils.Monitor monitor43 = new org.apache.storm.utils.Monitor();
        monitor43.setStream("hi!");
        monitor43.setTopology("hi!");
        monitor43.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface50 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor43.metrics(iface50);
        monitor43.setComponent("");
        org.apache.storm.utils.Monitor monitor54 = new org.apache.storm.utils.Monitor();
        monitor54.setStream("hi!");
        monitor54.setTopology("hi!");
        monitor54.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface61 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor54.metrics(iface61);
        monitor43.metrics(iface61);
        monitor22.metrics(iface61);
        monitor7.metrics(iface61);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface61);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor7);
        org.junit.Assert.assertNotNull(iface20);
        org.junit.Assert.assertNotNull(monitor22);
        org.junit.Assert.assertNotNull(iface50);
        org.junit.Assert.assertNotNull(iface61);
    }

    @Test
    public void test0679() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0679");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setStream("");
        monitor15.setWatch("hi!");
        monitor15.setInterval((int) (short) 0);
        monitor15.setComponent("");
        monitor15.setStream("hi!");
        monitor15.setInterval((int) (byte) 1);
        org.apache.storm.utils.Monitor monitor32 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor32.setInterval((int) (short) -1);
        monitor32.setInterval((int) 'a');
        monitor32.setInterval((int) ' ');
        monitor32.setComponent("");
        monitor32.setTopology("hi!");
        monitor32.setInterval(10);
        org.apache.storm.utils.Monitor monitor45 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor45.setInterval((int) (short) -1);
        monitor45.setInterval((int) 'a');
        monitor45.setInterval((int) ' ');
        monitor45.setComponent("");
        monitor45.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface56 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor45.metrics(iface56);
        monitor32.metrics(iface56);
        monitor15.metrics(iface56);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface56);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(monitor32);
        org.junit.Assert.assertNotNull(monitor45);
        org.junit.Assert.assertNotNull(iface56);
    }

    @Test
    public void test0680() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0680");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setComponent("");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0681() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0681");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0682() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0682");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setInterval((int) '4');
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setInterval((int) ' ');
        monitor13.setStream("");
        monitor13.setWatch("hi!");
        monitor13.setComponent("");
        monitor13.setStream("hi!");
        org.apache.storm.utils.Monitor monitor28 = new org.apache.storm.utils.Monitor();
        monitor28.setStream("hi!");
        monitor28.setTopology("hi!");
        monitor28.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface35 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor28.metrics(iface35);
        monitor28.setComponent("");
        org.apache.storm.utils.Monitor monitor39 = new org.apache.storm.utils.Monitor();
        monitor39.setStream("hi!");
        monitor39.setTopology("hi!");
        monitor39.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface46 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor39.metrics(iface46);
        monitor28.metrics(iface46);
        monitor13.metrics(iface46);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface46);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface35);
        org.junit.Assert.assertNotNull(iface46);
    }

    @Test
    public void test0683() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0683");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setInterval((int) (byte) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0684() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0684");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        org.apache.storm.utils.Monitor monitor19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor19.setInterval((int) (short) -1);
        monitor19.setInterval((int) 'a');
        monitor19.setInterval((int) ' ');
        monitor19.setStream("");
        monitor19.setWatch("hi!");
        monitor19.setComponent("");
        monitor19.setWatch("");
        monitor19.setComponent("");
        org.apache.storm.utils.Monitor monitor36 = new org.apache.storm.utils.Monitor();
        monitor36.setStream("hi!");
        monitor36.setTopology("hi!");
        monitor36.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface43 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor36.metrics(iface43);
        monitor19.metrics(iface43);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface43);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor19);
        org.junit.Assert.assertNotNull(iface43);
    }

    @Test
    public void test0685() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0685");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0686() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0686");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setInterval(100);
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0687() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0687");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0688() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0688");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0689() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0689");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass23 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass23);
    }

    @Test
    public void test0690() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0690");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0691() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0691");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0692() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0692");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) 1);
        org.apache.storm.utils.Monitor monitor23 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor23.setInterval((int) (short) -1);
        monitor23.setInterval((int) 'a');
        monitor23.setInterval((int) ' ');
        monitor23.setStream("");
        monitor23.setWatch("hi!");
        monitor23.setComponent("");
        monitor23.setStream("hi!");
        org.apache.storm.utils.Monitor monitor38 = new org.apache.storm.utils.Monitor();
        monitor38.setStream("hi!");
        monitor38.setTopology("hi!");
        monitor38.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface45 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor38.metrics(iface45);
        monitor38.setComponent("");
        org.apache.storm.utils.Monitor monitor49 = new org.apache.storm.utils.Monitor();
        monitor49.setStream("hi!");
        monitor49.setTopology("hi!");
        monitor49.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface56 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor49.metrics(iface56);
        monitor38.metrics(iface56);
        monitor23.metrics(iface56);
        org.apache.storm.utils.Monitor monitor60 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor60.setInterval((int) (short) -1);
        monitor60.setInterval((int) 'a');
        monitor60.setInterval((int) ' ');
        monitor60.setComponent("");
        monitor60.setTopology("hi!");
        monitor60.setInterval(10);
        org.apache.storm.utils.Monitor monitor73 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor73.setInterval((int) (short) -1);
        monitor73.setInterval((int) 'a');
        monitor73.setInterval((int) ' ');
        monitor73.setComponent("");
        monitor73.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface84 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor73.metrics(iface84);
        monitor60.metrics(iface84);
        monitor23.metrics(iface84);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface84);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor23);
        org.junit.Assert.assertNotNull(iface45);
        org.junit.Assert.assertNotNull(iface56);
        org.junit.Assert.assertNotNull(monitor60);
        org.junit.Assert.assertNotNull(monitor73);
        org.junit.Assert.assertNotNull(iface84);
    }

    @Test
    public void test0693() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0693");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        org.apache.storm.utils.Monitor monitor5 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor5.setStream("");
        monitor5.setTopology("");
        monitor5.setTopology("hi!");
        monitor5.setInterval(100);
        monitor5.setStream("");
        monitor5.setComponent("");
        org.apache.storm.utils.Monitor monitor18 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor18.setTopology("hi!");
        monitor18.setComponent("");
        monitor18.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor25 = new org.apache.storm.utils.Monitor();
        monitor25.setStream("hi!");
        monitor25.setTopology("hi!");
        monitor25.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface32 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor25.metrics(iface32);
        monitor18.metrics(iface32);
        monitor5.metrics(iface32);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface32);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor5);
        org.junit.Assert.assertNotNull(monitor18);
        org.junit.Assert.assertNotNull(iface32);
    }

    @Test
    public void test0694() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0694");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0695() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0695");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0696() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0696");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0697() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0697");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
    }

    @Test
    public void test0698() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0698");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setInterval(0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0699() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0699");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setInterval((int) (byte) 0);
        monitor0.setWatch("hi!");
        monitor0.setInterval(0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0700() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0700");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0701() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0701");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0702() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0702");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor7 = new org.apache.storm.utils.Monitor();
        monitor7.setStream("hi!");
        monitor7.setTopology("hi!");
        monitor7.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface14 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor7.metrics(iface14);
        monitor0.metrics(iface14);
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface14);
    }

    @Test
    public void test0703() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0703");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setInterval((int) '4');
        monitor0.setInterval((int) (short) 100);
        monitor0.setInterval(0);
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0704() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0704");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
    }

    @Test
    public void test0705() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0705");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0706() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0706");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0707() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0707");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0708() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0708");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0709() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0709");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setInterval(0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0710() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0710");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0711() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0711");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0712() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0712");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0713() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0713");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor0.metrics(iface7);
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(iface7);
    }

    @Test
    public void test0714() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0714");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0715() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0715");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setInterval((int) '4');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0716() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0716");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0717() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0717");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0718() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0718");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) '#');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0719() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0719");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("hi!");
        org.apache.storm.generated.Nimbus.Iface iface17 = null;
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface17);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0720() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0720");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0721() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0721");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor17.setTopology("");
        monitor17.setComponent("hi!");
        monitor17.setStream("");
        monitor17.setComponent("");
        monitor17.setTopology("");
        monitor17.setStream("");
        monitor17.setWatch("hi!");
        monitor17.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface34 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor17.metrics(iface34);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface34);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor17);
        org.junit.Assert.assertNotNull(iface34);
    }

    @Test
    public void test0722() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0722");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0723() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0723");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (byte) 1);
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0724() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0724");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0725() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0725");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0726() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0726");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0727() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0727");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0728() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0728");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setInterval(10);
        monitor0.setInterval(100);
        monitor0.setInterval(10);
        monitor0.setTopology("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0729() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0729");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0730() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0730");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0731() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0731");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0732() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0732");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) -1);
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0733() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0733");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval(0);
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0734() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0734");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0735() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0735");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0736() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0736");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor0.metrics(iface11);
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        org.apache.storm.utils.Monitor monitor17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor17.setInterval((int) (short) -1);
        monitor17.setInterval((int) 'a');
        monitor17.setInterval((int) ' ');
        monitor17.setComponent("");
        monitor17.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface28 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor17.metrics(iface28);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface28);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: hi! not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
        org.junit.Assert.assertNotNull(monitor17);
        org.junit.Assert.assertNotNull(iface28);
    }

    @Test
    public void test0737() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0737");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setStream("");
        monitor0.setInterval(0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0738() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0738");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0739() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0739");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0740() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0740");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0741() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0741");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setInterval((int) '4');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setInterval(10);
        java.lang.Class<?> wildcardClass25 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass25);
    }

    @Test
    public void test0742() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0742");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0743() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0743");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0744() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0744");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 10);
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0745() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0745");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        org.apache.storm.utils.Monitor monitor17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor17.setInterval((int) (short) -1);
        monitor17.setInterval((int) 'a');
        monitor17.setStream("");
        monitor17.setWatch("hi!");
        monitor17.setInterval((int) (short) 0);
        monitor17.setComponent("");
        monitor17.setStream("");
        monitor17.setInterval((int) '4');
        monitor17.setStream("");
        monitor17.setWatch("hi!");
        monitor17.setComponent("");
        monitor17.setStream("hi!");
        org.apache.storm.utils.Monitor monitor42 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor42.setInterval((int) (short) -1);
        monitor42.setInterval((int) 'a');
        monitor42.setInterval((int) ' ');
        monitor42.setComponent("");
        monitor42.setTopology("hi!");
        monitor42.setComponent("hi!");
        monitor42.setComponent("");
        monitor42.setStream("");
        monitor42.setComponent("");
        monitor42.setWatch("");
        org.apache.storm.utils.Monitor monitor63 = new org.apache.storm.utils.Monitor();
        monitor63.setStream("hi!");
        monitor63.setTopology("hi!");
        monitor63.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface70 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor63.metrics(iface70);
        monitor63.setComponent("");
        org.apache.storm.utils.Monitor monitor74 = new org.apache.storm.utils.Monitor();
        monitor74.setStream("hi!");
        monitor74.setTopology("hi!");
        monitor74.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface81 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor74.metrics(iface81);
        monitor63.metrics(iface81);
        monitor42.metrics(iface81);
        monitor17.metrics(iface81);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface81);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor17);
        org.junit.Assert.assertNotNull(monitor42);
        org.junit.Assert.assertNotNull(iface70);
        org.junit.Assert.assertNotNull(iface81);
    }

    @Test
    public void test0746() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0746");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0747() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0747");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0748() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0748");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setInterval((int) '4');
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass5 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0749() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0749");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 10);
    }

    @Test
    public void test0750() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0750");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (short) 100);
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0751() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0751");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (byte) 0);
    }

    @Test
    public void test0752() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0752");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        org.apache.storm.utils.Monitor monitor9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor9.setInterval((int) (short) -1);
        monitor9.setInterval((int) 'a');
        monitor9.setInterval((int) ' ');
        monitor9.setComponent("");
        monitor9.setTopology("hi!");
        monitor9.setInterval(10);
        org.apache.storm.utils.Monitor monitor22 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor22.setInterval((int) (short) -1);
        monitor22.setInterval((int) 'a');
        monitor22.setInterval((int) ' ');
        monitor22.setComponent("");
        monitor22.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface33 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor22.metrics(iface33);
        monitor9.metrics(iface33);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface33);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor9);
        org.junit.Assert.assertNotNull(monitor22);
        org.junit.Assert.assertNotNull(iface33);
    }

    @Test
    public void test0753() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0753");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        org.apache.storm.utils.Monitor monitor13 = new org.apache.storm.utils.Monitor();
        monitor13.setStream("hi!");
        monitor13.setTopology("hi!");
        monitor13.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface20 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor13.metrics(iface20);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface20);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface20);
    }

    @Test
    public void test0754() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0754");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setInterval((-1));
        org.apache.storm.utils.Monitor monitor7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor7.setTopology("hi!");
        monitor7.setComponent("");
        monitor7.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor14 = new org.apache.storm.utils.Monitor();
        monitor14.setStream("hi!");
        monitor14.setTopology("hi!");
        monitor14.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface21 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor14.metrics(iface21);
        monitor7.metrics(iface21);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface21);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor7);
        org.junit.Assert.assertNotNull(iface21);
    }

    @Test
    public void test0755() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0755");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0756() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0756");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0757() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0757");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0758() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0758");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0759() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0759");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) (byte) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0760() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0760");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setStream("");
        monitor15.setTopology("");
        monitor15.setTopology("hi!");
        monitor15.setInterval(100);
        monitor15.setStream("");
        monitor15.setComponent("");
        org.apache.storm.utils.Monitor monitor28 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor28.setTopology("hi!");
        monitor28.setComponent("");
        monitor28.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor35 = new org.apache.storm.utils.Monitor();
        monitor35.setStream("hi!");
        monitor35.setTopology("hi!");
        monitor35.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface42 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor35.metrics(iface42);
        monitor28.metrics(iface42);
        monitor15.metrics(iface42);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface42);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(monitor28);
        org.junit.Assert.assertNotNull(iface42);
    }

    @Test
    public void test0761() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0761");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0762() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0762");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setInterval(10);
        org.apache.storm.utils.Monitor monitor17 = new org.apache.storm.utils.Monitor();
        monitor17.setStream("hi!");
        monitor17.setTopology("hi!");
        monitor17.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface24 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor17.metrics(iface24);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface24);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface24);
    }

    @Test
    public void test0763() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0763");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setInterval((int) ' ');
        monitor15.setStream("");
        monitor15.setWatch("hi!");
        monitor15.setComponent("");
        monitor15.setWatch("");
        org.apache.storm.utils.Monitor monitor30 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor30.setInterval((int) (short) -1);
        monitor30.setInterval((int) 'a');
        monitor30.setStream("");
        monitor30.setWatch("");
        monitor30.setInterval((int) (byte) 1);
        monitor30.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface43 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor30.metrics(iface43);
        monitor15.metrics(iface43);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface43);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(monitor30);
        org.junit.Assert.assertNotNull(iface43);
    }

    @Test
    public void test0764() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0764");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0765() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0765");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0766() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0766");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0767() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0767");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0768() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0768");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setWatch("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0769() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0769");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0770() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0770");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        org.apache.storm.generated.Nimbus.Iface iface11 = null;
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0771() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0771");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        org.apache.storm.utils.Monitor monitor9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor9.setInterval((int) (short) -1);
        monitor9.setInterval((int) 'a');
        monitor9.setInterval((int) ' ');
        monitor9.setStream("");
        monitor9.setWatch("hi!");
        monitor9.setComponent("");
        monitor9.setStream("hi!");
        org.apache.storm.utils.Monitor monitor24 = new org.apache.storm.utils.Monitor();
        monitor24.setStream("hi!");
        monitor24.setTopology("hi!");
        monitor24.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface31 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor24.metrics(iface31);
        monitor24.setComponent("");
        org.apache.storm.utils.Monitor monitor35 = new org.apache.storm.utils.Monitor();
        monitor35.setStream("hi!");
        monitor35.setTopology("hi!");
        monitor35.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface42 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor35.metrics(iface42);
        monitor24.metrics(iface42);
        monitor9.metrics(iface42);
        org.apache.storm.utils.Monitor monitor46 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor46.setInterval((int) (short) -1);
        monitor46.setInterval((int) 'a');
        monitor46.setInterval((int) ' ');
        monitor46.setComponent("");
        monitor46.setTopology("hi!");
        monitor46.setInterval(10);
        org.apache.storm.utils.Monitor monitor59 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor59.setInterval((int) (short) -1);
        monitor59.setInterval((int) 'a');
        monitor59.setInterval((int) ' ');
        monitor59.setComponent("");
        monitor59.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface70 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor59.metrics(iface70);
        monitor46.metrics(iface70);
        monitor9.metrics(iface70);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface70);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor9);
        org.junit.Assert.assertNotNull(iface31);
        org.junit.Assert.assertNotNull(iface42);
        org.junit.Assert.assertNotNull(monitor46);
        org.junit.Assert.assertNotNull(monitor59);
        org.junit.Assert.assertNotNull(iface70);
    }

    @Test
    public void test0772() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0772");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setInterval((int) (short) 10);
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0773() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0773");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0774() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0774");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor23 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor23.setInterval((int) (short) -1);
        monitor23.setInterval((int) 'a');
        monitor23.setInterval((int) ' ');
        monitor23.setStream("");
        monitor23.setWatch("hi!");
        monitor23.setComponent("");
        monitor23.setStream("hi!");
        org.apache.storm.utils.Monitor monitor38 = new org.apache.storm.utils.Monitor();
        monitor38.setStream("hi!");
        monitor38.setTopology("hi!");
        monitor38.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface45 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor38.metrics(iface45);
        monitor38.setComponent("");
        org.apache.storm.utils.Monitor monitor49 = new org.apache.storm.utils.Monitor();
        monitor49.setStream("hi!");
        monitor49.setTopology("hi!");
        monitor49.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface56 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor49.metrics(iface56);
        monitor38.metrics(iface56);
        monitor23.metrics(iface56);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface56);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor23);
        org.junit.Assert.assertNotNull(iface45);
        org.junit.Assert.assertNotNull(iface56);
    }

    @Test
    public void test0775() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0775");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) 0);
        java.lang.Class<?> wildcardClass21 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test0776() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0776");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setInterval((int) (byte) 100);
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0777() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0777");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setInterval(100);
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0778() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0778");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0779() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0779");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setComponent("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0780() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0780");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0781() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0781");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0782() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0782");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0783() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0783");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("hi!");
        monitor0.setInterval(0);
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0784() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0784");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setInterval(100);
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
    }

    @Test
    public void test0785() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0785");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) -1);
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0786() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0786");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setWatch("");
        monitor0.setComponent("");
    }

    @Test
    public void test0787() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0787");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0788() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0788");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0789() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0789");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0790() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0790");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor21 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor21.setInterval((int) (short) -1);
        monitor21.setInterval((int) 'a');
        monitor21.setInterval((int) ' ');
        monitor21.setComponent("");
        monitor21.setTopology("hi!");
        monitor21.setComponent("hi!");
        monitor21.setComponent("");
        monitor21.setStream("");
        monitor21.setComponent("");
        monitor21.setWatch("");
        org.apache.storm.utils.Monitor monitor42 = new org.apache.storm.utils.Monitor();
        monitor42.setStream("hi!");
        monitor42.setTopology("hi!");
        monitor42.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface49 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor42.metrics(iface49);
        monitor42.setComponent("");
        org.apache.storm.utils.Monitor monitor53 = new org.apache.storm.utils.Monitor();
        monitor53.setStream("hi!");
        monitor53.setTopology("hi!");
        monitor53.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface60 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor53.metrics(iface60);
        monitor42.metrics(iface60);
        monitor21.metrics(iface60);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface60);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor21);
        org.junit.Assert.assertNotNull(iface49);
        org.junit.Assert.assertNotNull(iface60);
    }

    @Test
    public void test0791() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0791");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0792() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0792");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setInterval((int) (short) 10);
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0793() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0793");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0794() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0794");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0795() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0795");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setInterval(1);
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0796() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0796");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0797() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0797");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0798() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0798");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0799() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0799");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setInterval(0);
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0800() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0800");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) ' ');
        monitor0.setStream("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0801() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0801");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0802() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0802");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0803() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0803");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setWatch("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0804() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0804");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0805() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0805");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setInterval(100);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
    }

    @Test
    public void test0806() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0806");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setInterval((int) (byte) -1);
        monitor0.setInterval((int) (byte) 10);
        monitor0.setComponent("");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setInterval((int) ' ');
        monitor13.setStream("");
        monitor13.setWatch("hi!");
        monitor13.setComponent("");
        monitor13.setComponent("");
        monitor13.setWatch("");
        org.apache.storm.utils.Monitor monitor30 = new org.apache.storm.utils.Monitor();
        monitor30.setStream("hi!");
        monitor30.setTopology("hi!");
        monitor30.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface37 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor30.metrics(iface37);
        monitor13.metrics(iface37);
        monitor0.metrics(iface37);
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface37);
    }

    @Test
    public void test0807() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0807");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) -1);
        monitor0.setStream("");
        org.apache.storm.utils.Monitor monitor17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor17.setInterval((int) (short) -1);
        monitor17.setInterval((int) 'a');
        monitor17.setInterval((int) ' ');
        monitor17.setStream("");
        monitor17.setWatch("hi!");
        monitor17.setComponent("");
        monitor17.setWatch("");
        monitor17.setComponent("");
        org.apache.storm.utils.Monitor monitor34 = new org.apache.storm.utils.Monitor();
        monitor34.setStream("hi!");
        monitor34.setTopology("hi!");
        monitor34.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface41 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor34.metrics(iface41);
        monitor17.metrics(iface41);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface41);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor17);
        org.junit.Assert.assertNotNull(iface41);
    }

    @Test
    public void test0808() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0808");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0809() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0809");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass21 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test0810() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0810");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setInterval((int) (byte) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0811() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0811");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setInterval((int) (byte) 100);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0812() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0812");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setInterval((int) '4');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0813() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0813");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0814() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0814");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0815() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0815");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setStream("");
        monitor0.setInterval(1);
        monitor0.setStream("");
        monitor0.setInterval((int) (short) 100);
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0816() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0816");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setInterval(0);
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0817() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0817");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) '4');
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0818() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0818");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0819() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0819");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0820() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0820");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) '4');
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setInterval((-1));
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0821() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0821");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0822() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0822");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor0.metrics(iface17);
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface17);
    }

    @Test
    public void test0823() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0823");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setInterval((int) '#');
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setInterval((int) ' ');
        monitor13.setComponent("");
        monitor13.setTopology("hi!");
        monitor13.setInterval(10);
        org.apache.storm.utils.Monitor monitor26 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor26.setInterval((int) (short) -1);
        monitor26.setInterval((int) 'a');
        monitor26.setInterval((int) ' ');
        monitor26.setComponent("");
        monitor26.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface37 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor26.metrics(iface37);
        monitor13.metrics(iface37);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface37);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(monitor26);
        org.junit.Assert.assertNotNull(iface37);
    }

    @Test
    public void test0824() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0824");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0825() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0825");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0826() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0826");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor0.metrics(iface11);
        monitor0.setComponent("hi!");
        monitor0.setInterval(10);
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0827() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0827");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) 1);
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0828() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0828");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setStream("");
        monitor13.setWatch("");
        monitor13.setInterval((int) (byte) 1);
        monitor13.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface26 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor13.metrics(iface26);
        org.apache.storm.utils.Monitor monitor28 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor28.setInterval((int) (short) -1);
        monitor28.setInterval((int) 'a');
        monitor28.setInterval((int) ' ');
        monitor28.setComponent("");
        monitor28.setTopology("hi!");
        monitor28.setComponent("hi!");
        monitor28.setComponent("");
        monitor28.setStream("");
        monitor28.setComponent("");
        monitor28.setWatch("");
        org.apache.storm.utils.Monitor monitor49 = new org.apache.storm.utils.Monitor();
        monitor49.setStream("hi!");
        monitor49.setTopology("hi!");
        monitor49.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface56 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor49.metrics(iface56);
        monitor49.setComponent("");
        org.apache.storm.utils.Monitor monitor60 = new org.apache.storm.utils.Monitor();
        monitor60.setStream("hi!");
        monitor60.setTopology("hi!");
        monitor60.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface67 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor60.metrics(iface67);
        monitor49.metrics(iface67);
        monitor28.metrics(iface67);
        monitor13.metrics(iface67);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface67);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface26);
        org.junit.Assert.assertNotNull(monitor28);
        org.junit.Assert.assertNotNull(iface56);
        org.junit.Assert.assertNotNull(iface67);
    }

    @Test
    public void test0829() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0829");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("");
        monitor0.setComponent("");
        org.apache.storm.utils.Monitor monitor17 = new org.apache.storm.utils.Monitor();
        monitor17.setStream("hi!");
        monitor17.setTopology("hi!");
        monitor17.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface24 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor17.metrics(iface24);
        monitor17.setComponent("");
        org.apache.storm.utils.Monitor monitor28 = new org.apache.storm.utils.Monitor();
        monitor28.setStream("hi!");
        monitor28.setTopology("hi!");
        monitor28.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface35 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor28.metrics(iface35);
        monitor17.metrics(iface35);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface35);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface24);
        org.junit.Assert.assertNotNull(iface35);
    }

    @Test
    public void test0830() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0830");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        org.apache.storm.utils.Monitor monitor9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor9.setInterval((int) (short) -1);
        monitor9.setInterval((int) 'a');
        monitor9.setStream("");
        monitor9.setWatch("hi!");
        monitor9.setInterval((int) (short) 0);
        monitor9.setComponent("");
        monitor9.setStream("");
        monitor9.setInterval((int) '4');
        monitor9.setStream("");
        monitor9.setWatch("hi!");
        monitor9.setComponent("");
        monitor9.setStream("hi!");
        monitor9.setTopology("hi!");
        monitor9.setWatch("");
        org.apache.storm.utils.Monitor monitor38 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor38.setTopology("hi!");
        monitor38.setComponent("");
        org.apache.storm.utils.Monitor monitor43 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor43.setTopology("");
        monitor43.setComponent("hi!");
        monitor43.setStream("");
        monitor43.setComponent("");
        monitor43.setTopology("");
        monitor43.setStream("");
        monitor43.setWatch("hi!");
        monitor43.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface60 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor43.metrics(iface60);
        monitor38.metrics(iface60);
        monitor9.metrics(iface60);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface60);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor9);
        org.junit.Assert.assertNotNull(monitor38);
        org.junit.Assert.assertNotNull(monitor43);
        org.junit.Assert.assertNotNull(iface60);
    }

    @Test
    public void test0831() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0831");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0832() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0832");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0833() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0833");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setInterval((int) '4');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 100);
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0834() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0834");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0835() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0835");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setInterval((int) (short) 0);
        monitor0.setInterval((int) (byte) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0836() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0836");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        monitor0.setWatch("");
        monitor0.setStream("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0837() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0837");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) '#');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0838() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0838");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0839() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0839");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0840() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0840");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0841() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0841");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setInterval((int) '4');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0842() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0842");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0843() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0843");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setWatch("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        org.apache.storm.generated.Nimbus.Iface iface15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface15);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface15);
    }

    @Test
    public void test0844() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0844");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 1);
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0845() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0845");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setTopology("");
        monitor15.setComponent("hi!");
        monitor15.setStream("");
        monitor15.setComponent("");
        monitor15.setTopology("");
        monitor15.setStream("");
        monitor15.setWatch("hi!");
        monitor15.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface32 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor15.metrics(iface32);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface32);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(iface32);
    }

    @Test
    public void test0846() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0846");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0847() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0847");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0848() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0848");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setComponent("");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0849() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0849");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor0.metrics(iface7);
        monitor0.setComponent("");
        org.apache.storm.utils.Monitor monitor11 = new org.apache.storm.utils.Monitor();
        monitor11.setStream("hi!");
        monitor11.setTopology("hi!");
        monitor11.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface18 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor11.metrics(iface18);
        monitor0.metrics(iface18);
        monitor0.setInterval((int) (short) 1);
        org.junit.Assert.assertNotNull(iface7);
        org.junit.Assert.assertNotNull(iface18);
    }

    @Test
    public void test0850() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0850");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setWatch("");
        org.apache.storm.utils.Monitor monitor17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor17.setInterval((int) (short) -1);
        monitor17.setInterval((int) 'a');
        monitor17.setInterval((int) ' ');
        monitor17.setInterval((int) (byte) -1);
        monitor17.setInterval((int) (byte) 10);
        monitor17.setComponent("");
        org.apache.storm.utils.Monitor monitor30 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor30.setInterval((int) (short) -1);
        monitor30.setInterval((int) 'a');
        monitor30.setInterval((int) ' ');
        monitor30.setStream("");
        monitor30.setWatch("hi!");
        monitor30.setComponent("");
        monitor30.setComponent("");
        monitor30.setWatch("");
        org.apache.storm.utils.Monitor monitor47 = new org.apache.storm.utils.Monitor();
        monitor47.setStream("hi!");
        monitor47.setTopology("hi!");
        monitor47.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface54 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor47.metrics(iface54);
        monitor30.metrics(iface54);
        monitor17.metrics(iface54);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface54);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor17);
        org.junit.Assert.assertNotNull(monitor30);
        org.junit.Assert.assertNotNull(iface54);
    }

    @Test
    public void test0851() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0851");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setStream("");
        org.apache.storm.utils.Monitor monitor19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor19.setInterval((int) (short) -1);
        monitor19.setInterval((int) 'a');
        monitor19.setInterval((int) ' ');
        monitor19.setInterval((int) (byte) -1);
        monitor19.setInterval((int) (byte) 10);
        monitor19.setComponent("");
        org.apache.storm.utils.Monitor monitor32 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor32.setInterval((int) (short) -1);
        monitor32.setInterval((int) 'a');
        monitor32.setInterval((int) ' ');
        monitor32.setStream("");
        monitor32.setWatch("hi!");
        monitor32.setComponent("");
        monitor32.setComponent("");
        monitor32.setWatch("");
        org.apache.storm.utils.Monitor monitor49 = new org.apache.storm.utils.Monitor();
        monitor49.setStream("hi!");
        monitor49.setTopology("hi!");
        monitor49.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface56 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor49.metrics(iface56);
        monitor32.metrics(iface56);
        monitor19.metrics(iface56);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface56);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor19);
        org.junit.Assert.assertNotNull(monitor32);
        org.junit.Assert.assertNotNull(iface56);
    }

    @Test
    public void test0852() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0852");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        monitor0.setInterval((-1));
    }

    @Test
    public void test0853() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0853");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0854() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0854");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0855() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0855");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0856() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0856");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0857() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0857");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor0.metrics(iface7);
        monitor0.setComponent("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(iface7);
    }

    @Test
    public void test0858() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0858");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0859() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0859");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("hi!");
        monitor0.setInterval(0);
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setInterval((int) ' ');
        monitor15.setInterval((int) (byte) -1);
        monitor15.setInterval((int) (byte) 10);
        monitor15.setComponent("");
        org.apache.storm.utils.Monitor monitor28 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor28.setInterval((int) (short) -1);
        monitor28.setInterval((int) 'a');
        monitor28.setInterval((int) ' ');
        monitor28.setStream("");
        monitor28.setWatch("hi!");
        monitor28.setComponent("");
        monitor28.setComponent("");
        monitor28.setWatch("");
        org.apache.storm.utils.Monitor monitor45 = new org.apache.storm.utils.Monitor();
        monitor45.setStream("hi!");
        monitor45.setTopology("hi!");
        monitor45.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface52 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor45.metrics(iface52);
        monitor28.metrics(iface52);
        monitor15.metrics(iface52);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface52);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(monitor28);
        org.junit.Assert.assertNotNull(iface52);
    }

    @Test
    public void test0860() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0860");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0861() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0861");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0862() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0862");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor11.setInterval((int) (short) -1);
        monitor11.setInterval((int) 'a');
        monitor11.setInterval((int) ' ');
        monitor11.setStream("");
        monitor11.setWatch("hi!");
        monitor11.setComponent("");
        monitor11.setStream("hi!");
        org.apache.storm.utils.Monitor monitor26 = new org.apache.storm.utils.Monitor();
        monitor26.setStream("hi!");
        monitor26.setTopology("hi!");
        monitor26.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface33 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor26.metrics(iface33);
        monitor26.setComponent("");
        org.apache.storm.utils.Monitor monitor37 = new org.apache.storm.utils.Monitor();
        monitor37.setStream("hi!");
        monitor37.setTopology("hi!");
        monitor37.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface44 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor37.metrics(iface44);
        monitor26.metrics(iface44);
        monitor11.metrics(iface44);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface44);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(iface33);
        org.junit.Assert.assertNotNull(iface44);
    }

    @Test
    public void test0863() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0863");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor21 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor21.setInterval((int) (short) -1);
        monitor21.setInterval((int) 'a');
        monitor21.setInterval((int) ' ');
        monitor21.setComponent("");
        monitor21.setTopology("hi!");
        monitor21.setComponent("hi!");
        monitor21.setComponent("");
        monitor21.setStream("");
        monitor21.setComponent("");
        monitor21.setWatch("");
        org.apache.storm.utils.Monitor monitor42 = new org.apache.storm.utils.Monitor();
        monitor42.setStream("hi!");
        monitor42.setTopology("hi!");
        monitor42.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface49 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor42.metrics(iface49);
        monitor42.setComponent("");
        org.apache.storm.utils.Monitor monitor53 = new org.apache.storm.utils.Monitor();
        monitor53.setStream("hi!");
        monitor53.setTopology("hi!");
        monitor53.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface60 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor53.metrics(iface60);
        monitor42.metrics(iface60);
        monitor21.metrics(iface60);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface60);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor21);
        org.junit.Assert.assertNotNull(iface49);
        org.junit.Assert.assertNotNull(iface60);
    }

    @Test
    public void test0864() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0864");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) 100);
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0865() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0865");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.apache.storm.utils.Monitor monitor15 = new org.apache.storm.utils.Monitor();
        monitor15.setStream("hi!");
        monitor15.setTopology("hi!");
        monitor15.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface22 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor15.metrics(iface22);
        monitor15.setComponent("");
        org.apache.storm.utils.Monitor monitor26 = new org.apache.storm.utils.Monitor();
        monitor26.setStream("hi!");
        monitor26.setTopology("hi!");
        monitor26.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface33 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor26.metrics(iface33);
        monitor15.metrics(iface33);
        monitor0.metrics(iface33);
        org.apache.storm.utils.Monitor monitor37 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor37.setInterval((int) (short) -1);
        monitor37.setInterval((int) 'a');
        monitor37.setInterval((int) ' ');
        monitor37.setComponent("");
        monitor37.setTopology("hi!");
        monitor37.setInterval(10);
        org.apache.storm.utils.Monitor monitor50 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor50.setInterval((int) (short) -1);
        monitor50.setInterval((int) 'a');
        monitor50.setInterval((int) ' ');
        monitor50.setComponent("");
        monitor50.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface61 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor50.metrics(iface61);
        monitor37.metrics(iface61);
        monitor0.metrics(iface61);
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor67 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor67.setInterval((int) (short) -1);
        monitor67.setInterval((int) 'a');
        monitor67.setInterval((int) ' ');
        monitor67.setComponent("");
        monitor67.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface78 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor67.metrics(iface78);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface78);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface22);
        org.junit.Assert.assertNotNull(iface33);
        org.junit.Assert.assertNotNull(monitor37);
        org.junit.Assert.assertNotNull(monitor50);
        org.junit.Assert.assertNotNull(iface61);
        org.junit.Assert.assertNotNull(monitor67);
        org.junit.Assert.assertNotNull(iface78);
    }

    @Test
    public void test0866() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0866");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0867() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0867");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor0.metrics(iface11);
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) ' ');
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0868() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0868");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setInterval((int) (short) 10);
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0869() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0869");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0870() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0870");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setInterval(100);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0871() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0871");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setInterval(0);
        monitor0.setInterval(10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0872() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0872");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0873() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0873");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("");
        monitor0.setInterval(0);
        org.apache.storm.utils.Monitor monitor17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor17.setInterval((int) (short) -1);
        monitor17.setInterval((int) 'a');
        monitor17.setStream("");
        monitor17.setWatch("");
        monitor17.setInterval((int) (byte) 1);
        monitor17.setComponent("");
        monitor17.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor32 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor32.setInterval((int) (short) -1);
        monitor32.setInterval((int) 'a');
        monitor32.setInterval((int) ' ');
        monitor32.setStream("");
        monitor32.setWatch("hi!");
        monitor32.setComponent("");
        monitor32.setComponent("");
        monitor32.setWatch("");
        org.apache.storm.utils.Monitor monitor49 = new org.apache.storm.utils.Monitor();
        monitor49.setStream("hi!");
        monitor49.setTopology("hi!");
        monitor49.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface56 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor49.metrics(iface56);
        monitor32.metrics(iface56);
        monitor17.metrics(iface56);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface56);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor17);
        org.junit.Assert.assertNotNull(monitor32);
        org.junit.Assert.assertNotNull(iface56);
    }

    @Test
    public void test0874() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0874");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) 0);
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0875() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0875");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setInterval((int) '4');
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        org.apache.storm.utils.Monitor monitor21 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor21.setTopology("hi!");
        monitor21.setComponent("");
        monitor21.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor28 = new org.apache.storm.utils.Monitor();
        monitor28.setStream("hi!");
        monitor28.setTopology("hi!");
        monitor28.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface35 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor28.metrics(iface35);
        monitor21.metrics(iface35);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface35);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor21);
        org.junit.Assert.assertNotNull(iface35);
    }

    @Test
    public void test0876() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0876");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setInterval((int) '#');
        monitor0.setInterval((int) (byte) 0);
        monitor0.setInterval((int) (short) 10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0877() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0877");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) '4');
        monitor0.setInterval(0);
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0878() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0878");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0879() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0879");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setInterval(100);
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        monitor0.setWatch("hi!");
    }

    @Test
    public void test0880() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0880");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0881() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0881");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("hi!");
        monitor0.setInterval(0);
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) 'a');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0882() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0882");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setStream("");
    }

    @Test
    public void test0883() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0883");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        org.apache.storm.utils.Monitor monitor19 = new org.apache.storm.utils.Monitor();
        monitor19.setStream("hi!");
        monitor19.setTopology("hi!");
        monitor19.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface26 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor19.metrics(iface26);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface26);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface26);
    }

    @Test
    public void test0884() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0884");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setInterval(1);
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0885() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0885");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0886() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0886");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setWatch("");
        org.apache.storm.generated.Nimbus.Iface iface19 = null;
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface19);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0887() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0887");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
    }

    @Test
    public void test0888() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0888");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(10);
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setInterval((int) ' ');
        monitor13.setComponent("");
        monitor13.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface24 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor13.metrics(iface24);
        monitor0.metrics(iface24);
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface24);
    }

    @Test
    public void test0889() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0889");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0890() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0890");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("");
    }

    @Test
    public void test0891() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0891");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0892() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0892");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setInterval(10);
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0893() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0893");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (byte) 1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0894() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0894");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setInterval((int) (short) 100);
        monitor0.setInterval(1);
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0895() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0895");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setStream("");
        monitor0.setInterval(1);
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) 10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0896() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0896");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0897() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0897");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0898() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0898");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (short) 100);
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0899() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0899");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        org.apache.storm.utils.Monitor monitor17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor17.setTopology("hi!");
        monitor17.setComponent("");
        org.apache.storm.utils.Monitor monitor22 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor22.setTopology("");
        monitor22.setComponent("hi!");
        monitor22.setStream("");
        monitor22.setComponent("");
        monitor22.setTopology("");
        monitor22.setStream("");
        monitor22.setWatch("hi!");
        monitor22.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface39 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor22.metrics(iface39);
        monitor17.metrics(iface39);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface39);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor17);
        org.junit.Assert.assertNotNull(monitor22);
        org.junit.Assert.assertNotNull(iface39);
    }

    @Test
    public void test0900() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0900");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0901() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0901");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface19);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface19);
    }

    @Test
    public void test0902() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0902");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor11.setInterval((int) (short) -1);
        monitor11.setInterval((int) 'a');
        monitor11.setTopology("");
        monitor11.setInterval((int) '4');
        monitor11.setTopology("hi!");
        monitor11.setStream("");
        monitor11.setComponent("");
        monitor11.setInterval((int) (short) 0);
        monitor11.setInterval((int) (byte) 100);
        org.apache.storm.utils.Monitor monitor30 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor30.setStream("");
        monitor30.setTopology("");
        monitor30.setTopology("hi!");
        monitor30.setInterval(100);
        monitor30.setStream("");
        monitor30.setComponent("");
        org.apache.storm.utils.Monitor monitor43 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor43.setTopology("hi!");
        monitor43.setComponent("");
        monitor43.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor50 = new org.apache.storm.utils.Monitor();
        monitor50.setStream("hi!");
        monitor50.setTopology("hi!");
        monitor50.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface57 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor50.metrics(iface57);
        monitor43.metrics(iface57);
        monitor30.metrics(iface57);
        monitor11.metrics(iface57);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface57);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(monitor30);
        org.junit.Assert.assertNotNull(monitor43);
        org.junit.Assert.assertNotNull(iface57);
    }

    @Test
    public void test0903() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0903");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0904() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0904");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) '#');
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0905() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0905");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        org.apache.storm.utils.Monitor monitor21 = new org.apache.storm.utils.Monitor();
        monitor21.setStream("hi!");
        monitor21.setTopology("hi!");
        monitor21.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface28 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor21.metrics(iface28);
        monitor21.setComponent("");
        org.apache.storm.utils.Monitor monitor32 = new org.apache.storm.utils.Monitor();
        monitor32.setStream("hi!");
        monitor32.setTopology("hi!");
        monitor32.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface39 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor32.metrics(iface39);
        monitor21.metrics(iface39);
        monitor0.metrics(iface39);
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface28);
        org.junit.Assert.assertNotNull(iface39);
    }

    @Test
    public void test0906() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0906");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0907() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0907");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0908() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0908");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0909() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0909");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setInterval((int) '4');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.apache.storm.utils.Monitor monitor25 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor25.setInterval((int) (short) -1);
        monitor25.setInterval((int) 'a');
        monitor25.setInterval((int) ' ');
        monitor25.setComponent("");
        monitor25.setTopology("hi!");
        monitor25.setComponent("hi!");
        monitor25.setComponent("");
        monitor25.setStream("");
        monitor25.setComponent("");
        monitor25.setWatch("");
        org.apache.storm.utils.Monitor monitor46 = new org.apache.storm.utils.Monitor();
        monitor46.setStream("hi!");
        monitor46.setTopology("hi!");
        monitor46.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface53 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor46.metrics(iface53);
        monitor46.setComponent("");
        org.apache.storm.utils.Monitor monitor57 = new org.apache.storm.utils.Monitor();
        monitor57.setStream("hi!");
        monitor57.setTopology("hi!");
        monitor57.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface64 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor57.metrics(iface64);
        monitor46.metrics(iface64);
        monitor25.metrics(iface64);
        monitor0.metrics(iface64);
        monitor0.setStream("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor25);
        org.junit.Assert.assertNotNull(iface53);
        org.junit.Assert.assertNotNull(iface64);
    }

    @Test
    public void test0910() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0910");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0911() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0911");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0912() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0912");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
    }

    @Test
    public void test0913() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0913");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("");
        org.apache.storm.generated.Nimbus.Iface iface17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface17);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface17);
    }

    @Test
    public void test0914() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0914");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 100);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0915() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0915");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        org.apache.storm.generated.Nimbus.Iface iface9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface9);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface9);
    }

    @Test
    public void test0916() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0916");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0917() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0917");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0918() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0918");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0919() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0919");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0920() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0920");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 100);
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0921() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0921");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0922() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0922");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0923() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0923");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setInterval((int) '4');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0924() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0924");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0925() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0925");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0926() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0926");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setInterval(10);
        monitor0.setInterval(100);
        monitor0.setInterval(10);
        monitor0.setInterval(100);
        monitor0.setInterval(0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0927() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0927");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setInterval(10);
        monitor0.setInterval(100);
        monitor0.setInterval(10);
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0928() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0928");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor0.metrics(iface7);
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(iface7);
    }

    @Test
    public void test0929() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0929");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0930() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0930");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0931() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0931");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setInterval(0);
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
    }

    @Test
    public void test0932() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0932");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0933() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0933");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor0.metrics(iface7);
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(iface7);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0934() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0934");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0935() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0935");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setInterval(10);
        monitor0.setWatch("");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0936() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0936");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0937() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0937");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0938() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0938");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setInterval((int) (byte) 100);
    }

    @Test
    public void test0939() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0939");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
    }

    @Test
    public void test0940() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0940");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setInterval((int) '4');
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0941() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0941");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0942() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0942");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0943() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0943");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0944() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0944");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setInterval((int) (byte) 1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0945() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0945");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.utils.Monitor monitor7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor7.setStream("");
        monitor7.setTopology("");
        monitor7.setTopology("hi!");
        monitor7.setInterval(100);
        monitor7.setStream("");
        monitor7.setComponent("");
        org.apache.storm.utils.Monitor monitor20 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor20.setTopology("hi!");
        monitor20.setComponent("");
        monitor20.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor27 = new org.apache.storm.utils.Monitor();
        monitor27.setStream("hi!");
        monitor27.setTopology("hi!");
        monitor27.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface34 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor27.metrics(iface34);
        monitor20.metrics(iface34);
        monitor7.metrics(iface34);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface34);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor7);
        org.junit.Assert.assertNotNull(monitor20);
        org.junit.Assert.assertNotNull(iface34);
    }

    @Test
    public void test0946() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0946");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setWatch("hi!");
        org.apache.storm.utils.Monitor monitor11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor11.setInterval((int) (short) -1);
        monitor11.setInterval((int) 'a');
        monitor11.setTopology("");
        monitor11.setInterval((int) '4');
        monitor11.setTopology("hi!");
        monitor11.setStream("");
        monitor11.setComponent("");
        monitor11.setInterval((int) (short) 0);
        monitor11.setInterval((int) (byte) 100);
        org.apache.storm.utils.Monitor monitor30 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor30.setStream("");
        monitor30.setTopology("");
        monitor30.setTopology("hi!");
        monitor30.setInterval(100);
        monitor30.setStream("");
        monitor30.setComponent("");
        org.apache.storm.utils.Monitor monitor43 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor43.setTopology("hi!");
        monitor43.setComponent("");
        monitor43.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor50 = new org.apache.storm.utils.Monitor();
        monitor50.setStream("hi!");
        monitor50.setTopology("hi!");
        monitor50.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface57 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor50.metrics(iface57);
        monitor43.metrics(iface57);
        monitor30.metrics(iface57);
        monitor11.metrics(iface57);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface57);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(monitor30);
        org.junit.Assert.assertNotNull(monitor43);
        org.junit.Assert.assertNotNull(iface57);
    }

    @Test
    public void test0947() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0947");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 1);
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) '#');
        monitor0.setInterval((int) '4');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0948() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0948");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0949() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0949");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setInterval((int) 'a');
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0950() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0950");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0951() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0951");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0952() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0952");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setTopology("");
        monitor15.setComponent("hi!");
        monitor15.setStream("");
        monitor15.setComponent("");
        monitor15.setTopology("");
        monitor15.setStream("");
        monitor15.setWatch("hi!");
        monitor15.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface32 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor15.metrics(iface32);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface32);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(iface32);
    }

    @Test
    public void test0953() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0953");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0954() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0954");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface15);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface15);
    }

    @Test
    public void test0955() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0955");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0956() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0956");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setInterval(0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0957() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0957");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setInterval(0);
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 1);
    }

    @Test
    public void test0958() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0958");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setStream("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0959() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0959");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setInterval(1);
        org.apache.storm.utils.Monitor monitor11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor11.setInterval((int) (short) -1);
        monitor11.setInterval((int) 'a');
        monitor11.setInterval((int) ' ');
        monitor11.setComponent("");
        monitor11.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface22 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor11.metrics(iface22);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface22);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(iface22);
    }

    @Test
    public void test0960() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0960");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0961() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0961");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0962() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0962");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setWatch("");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setInterval((int) ' ');
        monitor15.setInterval((int) (byte) -1);
        monitor15.setInterval((int) (byte) 10);
        monitor15.setComponent("");
        org.apache.storm.utils.Monitor monitor28 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor28.setInterval((int) (short) -1);
        monitor28.setInterval((int) 'a');
        monitor28.setInterval((int) ' ');
        monitor28.setStream("");
        monitor28.setWatch("hi!");
        monitor28.setComponent("");
        monitor28.setComponent("");
        monitor28.setWatch("");
        org.apache.storm.utils.Monitor monitor45 = new org.apache.storm.utils.Monitor();
        monitor45.setStream("hi!");
        monitor45.setTopology("hi!");
        monitor45.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface52 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor45.metrics(iface52);
        monitor28.metrics(iface52);
        monitor15.metrics(iface52);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface52);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(monitor28);
        org.junit.Assert.assertNotNull(iface52);
    }

    @Test
    public void test0963() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0963");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("hi!");
        monitor0.setInterval(0);
        monitor0.setInterval((int) (short) 10);
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0964() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0964");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0965() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0965");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        java.lang.Class<?> wildcardClass5 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0966() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0966");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setInterval((int) '4');
        org.apache.storm.utils.Monitor monitor3 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor3.setInterval((int) (short) -1);
        monitor3.setInterval((int) 'a');
        monitor3.setTopology("");
        monitor3.setInterval((int) '4');
        monitor3.setTopology("hi!");
        monitor3.setStream("");
        monitor3.setComponent("");
        monitor3.setInterval((int) (short) 0);
        monitor3.setInterval((int) (byte) 100);
        org.apache.storm.utils.Monitor monitor22 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor22.setStream("");
        monitor22.setTopology("");
        monitor22.setTopology("hi!");
        monitor22.setInterval(100);
        monitor22.setStream("");
        monitor22.setComponent("");
        org.apache.storm.utils.Monitor monitor35 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor35.setTopology("hi!");
        monitor35.setComponent("");
        monitor35.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor42 = new org.apache.storm.utils.Monitor();
        monitor42.setStream("hi!");
        monitor42.setTopology("hi!");
        monitor42.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface49 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor42.metrics(iface49);
        monitor35.metrics(iface49);
        monitor22.metrics(iface49);
        monitor3.metrics(iface49);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface49);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor3);
        org.junit.Assert.assertNotNull(monitor22);
        org.junit.Assert.assertNotNull(monitor35);
        org.junit.Assert.assertNotNull(iface49);
    }

    @Test
    public void test0967() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0967");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setInterval((int) (short) 100);
        monitor0.setInterval((int) (byte) -1);
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0968() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0968");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
    }

    @Test
    public void test0969() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0969");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setInterval(100);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0970() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0970");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setInterval((-1));
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0971() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0971");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setInterval(10);
        monitor0.setWatch("");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0972() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0972");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0973() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0973");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0974() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0974");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0975() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0975");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setInterval((int) (byte) -1);
        monitor0.setComponent("hi!");
        org.apache.storm.utils.Monitor monitor19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor19.setInterval((int) (short) -1);
        monitor19.setInterval((int) 'a');
        monitor19.setStream("");
        monitor19.setWatch("hi!");
        monitor19.setInterval((int) (short) 0);
        monitor19.setComponent("");
        monitor19.setStream("hi!");
        monitor19.setInterval((int) (byte) 1);
        org.apache.storm.utils.Monitor monitor36 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor36.setInterval((int) (short) -1);
        monitor36.setInterval((int) 'a');
        monitor36.setInterval((int) ' ');
        monitor36.setComponent("");
        monitor36.setTopology("hi!");
        monitor36.setInterval(10);
        org.apache.storm.utils.Monitor monitor49 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor49.setInterval((int) (short) -1);
        monitor49.setInterval((int) 'a');
        monitor49.setInterval((int) ' ');
        monitor49.setComponent("");
        monitor49.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface60 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor49.metrics(iface60);
        monitor36.metrics(iface60);
        monitor19.metrics(iface60);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface60);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor19);
        org.junit.Assert.assertNotNull(monitor36);
        org.junit.Assert.assertNotNull(monitor49);
        org.junit.Assert.assertNotNull(iface60);
    }

    @Test
    public void test0976() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0976");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setInterval((int) ' ');
        monitor13.setStream("");
        monitor13.setWatch("hi!");
        monitor13.setComponent("");
        monitor13.setStream("hi!");
        org.apache.storm.utils.Monitor monitor28 = new org.apache.storm.utils.Monitor();
        monitor28.setStream("hi!");
        monitor28.setTopology("hi!");
        monitor28.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface35 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor28.metrics(iface35);
        monitor28.setComponent("");
        org.apache.storm.utils.Monitor monitor39 = new org.apache.storm.utils.Monitor();
        monitor39.setStream("hi!");
        monitor39.setTopology("hi!");
        monitor39.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface46 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor39.metrics(iface46);
        monitor28.metrics(iface46);
        monitor13.metrics(iface46);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface46);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface35);
        org.junit.Assert.assertNotNull(iface46);
    }

    @Test
    public void test0977() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0977");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setInterval((int) '4');
        monitor0.setWatch("");
        monitor0.setWatch("");
        org.apache.storm.utils.Monitor monitor7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor7.setInterval((int) (short) -1);
        monitor7.setInterval((int) 'a');
        monitor7.setStream("");
        monitor7.setWatch("");
        monitor7.setInterval((int) (byte) 1);
        monitor7.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface20 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor7.metrics(iface20);
        org.apache.storm.utils.Monitor monitor22 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor22.setInterval((int) (short) -1);
        monitor22.setInterval((int) 'a');
        monitor22.setInterval((int) ' ');
        monitor22.setComponent("");
        monitor22.setTopology("hi!");
        monitor22.setComponent("hi!");
        monitor22.setComponent("");
        monitor22.setStream("");
        monitor22.setComponent("");
        monitor22.setWatch("");
        org.apache.storm.utils.Monitor monitor43 = new org.apache.storm.utils.Monitor();
        monitor43.setStream("hi!");
        monitor43.setTopology("hi!");
        monitor43.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface50 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor43.metrics(iface50);
        monitor43.setComponent("");
        org.apache.storm.utils.Monitor monitor54 = new org.apache.storm.utils.Monitor();
        monitor54.setStream("hi!");
        monitor54.setTopology("hi!");
        monitor54.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface61 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor54.metrics(iface61);
        monitor43.metrics(iface61);
        monitor22.metrics(iface61);
        monitor7.metrics(iface61);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface61);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor7);
        org.junit.Assert.assertNotNull(iface20);
        org.junit.Assert.assertNotNull(monitor22);
        org.junit.Assert.assertNotNull(iface50);
        org.junit.Assert.assertNotNull(iface61);
    }

    @Test
    public void test0978() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0978");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0979() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0979");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0980() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0980");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0981() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0981");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0982() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0982");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setInterval((int) '4');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0983() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0983");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0984() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0984");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) 100);
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0985() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0985");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0986() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0986");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0987() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0987");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 100);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0988() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0988");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0989() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0989");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0990() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0990");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval(0);
        java.lang.Class<?> wildcardClass21 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test0991() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0991");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 1);
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0992() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0992");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("");
        monitor0.setStream("");
        monitor0.setInterval(0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0993() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0993");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0994() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0994");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0995() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0995");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setTopology("");
        monitor0.setInterval((int) '#');
        monitor0.setWatch("hi!");
        org.apache.storm.utils.Monitor monitor19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor19.setTopology("");
        monitor19.setComponent("hi!");
        monitor19.setStream("hi!");
        monitor19.setTopology("hi!");
        monitor19.setStream("hi!");
        monitor19.setComponent("");
        org.apache.storm.utils.Monitor monitor32 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor32.setInterval((int) (short) -1);
        monitor32.setInterval((int) 'a');
        monitor32.setInterval((int) ' ');
        monitor32.setStream("");
        monitor32.setWatch("hi!");
        monitor32.setComponent("");
        monitor32.setStream("hi!");
        org.apache.storm.utils.Monitor monitor47 = new org.apache.storm.utils.Monitor();
        monitor47.setStream("hi!");
        monitor47.setTopology("hi!");
        monitor47.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface54 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor47.metrics(iface54);
        monitor47.setComponent("");
        org.apache.storm.utils.Monitor monitor58 = new org.apache.storm.utils.Monitor();
        monitor58.setStream("hi!");
        monitor58.setTopology("hi!");
        monitor58.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface65 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor58.metrics(iface65);
        monitor47.metrics(iface65);
        monitor32.metrics(iface65);
        monitor19.metrics(iface65);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface65);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor19);
        org.junit.Assert.assertNotNull(monitor32);
        org.junit.Assert.assertNotNull(iface54);
        org.junit.Assert.assertNotNull(iface65);
    }

    @Test
    public void test0996() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0996");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass23 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass23);
    }

    @Test
    public void test0997() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0997");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setInterval((int) (byte) -1);
        monitor0.setInterval((int) (byte) 10);
        monitor0.setInterval((int) 'a');
        org.apache.storm.utils.Monitor monitor13 = new org.apache.storm.utils.Monitor();
        monitor13.setStream("hi!");
        monitor13.setTopology("hi!");
        monitor13.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface20 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor13.metrics(iface20);
        monitor13.setWatch("hi!");
        monitor13.setComponent("");
        org.apache.storm.utils.Monitor monitor26 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor26.setInterval((int) (short) -1);
        monitor26.setInterval((int) 'a');
        monitor26.setInterval((int) ' ');
        monitor26.setStream("");
        monitor26.setWatch("hi!");
        monitor26.setComponent("");
        monitor26.setStream("hi!");
        org.apache.storm.utils.Monitor monitor41 = new org.apache.storm.utils.Monitor();
        monitor41.setStream("hi!");
        monitor41.setTopology("hi!");
        monitor41.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface48 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor41.metrics(iface48);
        monitor41.setComponent("");
        org.apache.storm.utils.Monitor monitor52 = new org.apache.storm.utils.Monitor();
        monitor52.setStream("hi!");
        monitor52.setTopology("hi!");
        monitor52.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface59 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor52.metrics(iface59);
        monitor41.metrics(iface59);
        monitor26.metrics(iface59);
        org.apache.storm.utils.Monitor monitor63 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor63.setInterval((int) (short) -1);
        monitor63.setInterval((int) 'a');
        monitor63.setInterval((int) ' ');
        monitor63.setComponent("");
        monitor63.setTopology("hi!");
        monitor63.setInterval(10);
        org.apache.storm.utils.Monitor monitor76 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor76.setInterval((int) (short) -1);
        monitor76.setInterval((int) 'a');
        monitor76.setInterval((int) ' ');
        monitor76.setComponent("");
        monitor76.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface87 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor76.metrics(iface87);
        monitor63.metrics(iface87);
        monitor26.metrics(iface87);
        monitor13.metrics(iface87);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface87);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: comp-1 not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface20);
        org.junit.Assert.assertNotNull(monitor26);
        org.junit.Assert.assertNotNull(iface48);
        org.junit.Assert.assertNotNull(iface59);
        org.junit.Assert.assertNotNull(monitor63);
        org.junit.Assert.assertNotNull(monitor76);
        org.junit.Assert.assertNotNull(iface87);
    }

    @Test
    public void test0998() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0998");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0999() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test0999");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) '#');
        monitor0.setInterval((int) '#');
        java.lang.Class<?> wildcardClass21 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test1000() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest1.test1000");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setStream("");
        monitor15.setWatch("");
        monitor15.setInterval((int) (byte) 1);
        monitor15.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface28 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor15.metrics(iface28);
        monitor0.metrics(iface28);
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(iface28);
    }
}

