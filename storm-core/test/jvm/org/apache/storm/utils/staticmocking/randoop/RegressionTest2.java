package org.apache.storm.utils.staticmocking.randoop;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RegressionTest2 {

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
    public void test1001() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1001");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setInterval(100);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1002() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1002");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
    }

    @Test
    public void test1003() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1003");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) 100);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1004() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1004");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (byte) 1);
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test1005() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1005");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1006() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1006");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setInterval(0);
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
    }

    @Test
    public void test1007() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1007");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1008() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1008");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setInterval(0);
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setComponent("");
    }

    @Test
    public void test1009() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1009");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1010() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1010");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test1011() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1011");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setInterval((int) 'a');
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) '#');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1012() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1012");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setTopology("");
        monitor13.setComponent("hi!");
        monitor13.setStream("hi!");
        monitor13.setTopology("hi!");
        monitor13.setStream("hi!");
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
        monitor13.metrics(iface59);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface59);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(monitor26);
        org.junit.Assert.assertNotNull(iface48);
        org.junit.Assert.assertNotNull(iface59);
    }

    @Test
    public void test1013() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1013");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1014() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1014");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1015() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1015");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1016() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1016");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1017() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1017");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor15 = new org.apache.storm.utils.Monitor();
        monitor15.setStream("hi!");
        monitor15.setTopology("hi!");
        monitor15.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface22 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor15.metrics(iface22);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface22);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(iface22);
    }

    @Test
    public void test1018() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1018");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1019() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1019");
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
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1020() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1020");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) '4');
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        org.apache.storm.utils.Monitor monitor11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor11.setTopology("hi!");
        monitor11.setComponent("");
        org.apache.storm.utils.Monitor monitor16 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor16.setTopology("");
        monitor16.setComponent("hi!");
        monitor16.setStream("");
        monitor16.setComponent("");
        monitor16.setTopology("");
        monitor16.setStream("");
        monitor16.setWatch("hi!");
        monitor16.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface33 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor16.metrics(iface33);
        monitor11.metrics(iface33);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface33);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(monitor16);
        org.junit.Assert.assertNotNull(iface33);
    }

    @Test
    public void test1021() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1021");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1022() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1022");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) 10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1023() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1023");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1024() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1024");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test1025() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1025");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1026() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1026");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1027() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1027");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1028() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1028");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test1029() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1029");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) 'a');
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test1030() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1030");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("");
        monitor0.setStream("");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setInterval((int) ' ');
        monitor13.setStream("");
        monitor13.setWatch("hi!");
        monitor13.setComponent("");
        monitor13.setWatch("");
        org.apache.storm.utils.Monitor monitor28 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor28.setInterval((int) (short) -1);
        monitor28.setInterval((int) 'a');
        monitor28.setStream("");
        monitor28.setWatch("");
        monitor28.setInterval((int) (byte) 1);
        monitor28.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface41 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor28.metrics(iface41);
        monitor13.metrics(iface41);
        monitor13.setWatch("hi!");
        org.apache.storm.generated.Nimbus.Iface iface46 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
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
        org.junit.Assert.assertNotNull(monitor28);
        org.junit.Assert.assertNotNull(iface41);
        org.junit.Assert.assertNotNull(iface46);
    }

    @Test
    public void test1031() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1031");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setTopology("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setStream("");
        monitor15.setWatch("hi!");
        monitor15.setInterval((int) (short) 0);
        monitor15.setComponent("");
        monitor15.setStream("");
        monitor15.setInterval((int) '4');
        monitor15.setStream("");
        monitor15.setWatch("hi!");
        monitor15.setComponent("");
        monitor15.setStream("hi!");
        monitor15.setTopology("hi!");
        monitor15.setWatch("");
        org.apache.storm.utils.Monitor monitor44 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor44.setTopology("hi!");
        monitor44.setComponent("");
        org.apache.storm.utils.Monitor monitor49 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor49.setTopology("");
        monitor49.setComponent("hi!");
        monitor49.setStream("");
        monitor49.setComponent("");
        monitor49.setTopology("");
        monitor49.setStream("");
        monitor49.setWatch("hi!");
        monitor49.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface66 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor49.metrics(iface66);
        monitor44.metrics(iface66);
        monitor15.metrics(iface66);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface66);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(monitor44);
        org.junit.Assert.assertNotNull(monitor49);
        org.junit.Assert.assertNotNull(iface66);
    }

    @Test
    public void test1032() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1032");
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
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass21 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test1033() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1033");
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
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test1034() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1034");
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
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1035() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1035");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor0.metrics(iface7);
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(iface7);
    }

    @Test
    public void test1036() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1036");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1037() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1037");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setWatch("hi!");
        org.apache.storm.utils.Monitor monitor19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor19.setInterval((int) (short) -1);
        monitor19.setInterval((int) 'a');
        monitor19.setInterval((int) ' ');
        monitor19.setStream("");
        monitor19.setWatch("hi!");
        monitor19.setComponent("");
        monitor19.setStream("hi!");
        org.apache.storm.utils.Monitor monitor34 = new org.apache.storm.utils.Monitor();
        monitor34.setStream("hi!");
        monitor34.setTopology("hi!");
        monitor34.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface41 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor34.metrics(iface41);
        monitor34.setComponent("");
        org.apache.storm.utils.Monitor monitor45 = new org.apache.storm.utils.Monitor();
        monitor45.setStream("hi!");
        monitor45.setTopology("hi!");
        monitor45.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface52 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor45.metrics(iface52);
        monitor34.metrics(iface52);
        monitor19.metrics(iface52);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface52);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor19);
        org.junit.Assert.assertNotNull(iface41);
        org.junit.Assert.assertNotNull(iface52);
    }

    @Test
    public void test1038() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1038");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor0.metrics(iface7);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
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
        monitor0.metrics(iface74);
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(iface7);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface35);
        org.junit.Assert.assertNotNull(iface46);
        org.junit.Assert.assertNotNull(monitor50);
        org.junit.Assert.assertNotNull(monitor63);
        org.junit.Assert.assertNotNull(iface74);
    }

    @Test
    public void test1039() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1039");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test1040() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1040");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor0.metrics(iface11);
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test1041() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1041");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) (short) 0);
        monitor0.setStream("");
    }

    @Test
    public void test1042() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1042");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) 100);
    }

    @Test
    public void test1043() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1043");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setInterval(1);
    }

    @Test
    public void test1044() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1044");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test1045() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1045");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1046() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1046");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 1);
        org.apache.storm.utils.Monitor monitor11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor11.setInterval((int) (short) -1);
        monitor11.setInterval((int) 'a');
        monitor11.setStream("");
        monitor11.setWatch("hi!");
        monitor11.setInterval((int) (short) 0);
        monitor11.setComponent("");
        monitor11.setStream("");
        monitor11.setInterval((int) '4');
        monitor11.setStream("");
        monitor11.setWatch("hi!");
        monitor11.setComponent("");
        monitor11.setStream("hi!");
        monitor11.setTopology("hi!");
        monitor11.setWatch("");
        org.apache.storm.utils.Monitor monitor40 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor40.setTopology("hi!");
        monitor40.setComponent("");
        org.apache.storm.utils.Monitor monitor45 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor45.setTopology("");
        monitor45.setComponent("hi!");
        monitor45.setStream("");
        monitor45.setComponent("");
        monitor45.setTopology("");
        monitor45.setStream("");
        monitor45.setWatch("hi!");
        monitor45.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface62 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor45.metrics(iface62);
        monitor40.metrics(iface62);
        monitor11.metrics(iface62);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface62);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(monitor40);
        org.junit.Assert.assertNotNull(monitor45);
        org.junit.Assert.assertNotNull(iface62);
    }

    @Test
    public void test1047() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1047");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 100);
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1048() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1048");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1049() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1049");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) '4');
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1050() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1050");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1051() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1051");
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
        org.apache.storm.utils.Monitor monitor23 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor23.setInterval((int) (short) -1);
        monitor23.setInterval((int) 'a');
        monitor23.setStream("");
        monitor23.setWatch("hi!");
        monitor23.setInterval((int) (short) 0);
        monitor23.setComponent("");
        monitor23.setStream("hi!");
        monitor23.setInterval((int) (byte) 1);
        org.apache.storm.utils.Monitor monitor40 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor40.setInterval((int) (short) -1);
        monitor40.setInterval((int) 'a');
        monitor40.setInterval((int) ' ');
        monitor40.setComponent("");
        monitor40.setTopology("hi!");
        monitor40.setInterval(10);
        org.apache.storm.utils.Monitor monitor53 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor53.setInterval((int) (short) -1);
        monitor53.setInterval((int) 'a');
        monitor53.setInterval((int) ' ');
        monitor53.setComponent("");
        monitor53.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface64 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor53.metrics(iface64);
        monitor40.metrics(iface64);
        monitor23.metrics(iface64);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface64);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor23);
        org.junit.Assert.assertNotNull(monitor40);
        org.junit.Assert.assertNotNull(monitor53);
        org.junit.Assert.assertNotNull(iface64);
    }

    @Test
    public void test1052() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1052");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test1053() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1053");
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
        java.lang.Class<?> wildcardClass27 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass27);
    }

    @Test
    public void test1054() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1054");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setWatch("");
        org.apache.storm.utils.Monitor monitor9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor9.setTopology("");
        monitor9.setComponent("hi!");
        monitor9.setStream("");
        monitor9.setComponent("");
        monitor9.setTopology("");
        monitor9.setStream("");
        monitor9.setWatch("hi!");
        monitor9.setTopology("hi!");
        monitor9.setComponent("");
        org.apache.storm.utils.Monitor monitor28 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor28.setInterval((int) (short) -1);
        monitor28.setInterval((int) 'a');
        monitor28.setInterval((int) ' ');
        monitor28.setStream("");
        monitor28.setWatch("hi!");
        monitor28.setComponent("");
        monitor28.setWatch("");
        org.apache.storm.utils.Monitor monitor43 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor43.setInterval((int) (short) -1);
        monitor43.setInterval((int) 'a');
        monitor43.setStream("");
        monitor43.setWatch("");
        monitor43.setInterval((int) (byte) 1);
        monitor43.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface56 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor43.metrics(iface56);
        monitor28.metrics(iface56);
        monitor9.metrics(iface56);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface56);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor9);
        org.junit.Assert.assertNotNull(monitor28);
        org.junit.Assert.assertNotNull(monitor43);
        org.junit.Assert.assertNotNull(iface56);
    }

    @Test
    public void test1055() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1055");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setTopology("");
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) '4');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1056() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1056");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1057() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1057");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) 'a');
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1058() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1058");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor17.setInterval((int) (short) -1);
        monitor17.setInterval((int) 'a');
        monitor17.setInterval((int) ' ');
        monitor17.setStream("");
        monitor17.setWatch("hi!");
        monitor17.setComponent("");
        monitor17.setStream("hi!");
        org.apache.storm.utils.Monitor monitor32 = new org.apache.storm.utils.Monitor();
        monitor32.setStream("hi!");
        monitor32.setTopology("hi!");
        monitor32.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface39 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor32.metrics(iface39);
        monitor32.setComponent("");
        org.apache.storm.utils.Monitor monitor43 = new org.apache.storm.utils.Monitor();
        monitor43.setStream("hi!");
        monitor43.setTopology("hi!");
        monitor43.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface50 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor43.metrics(iface50);
        monitor32.metrics(iface50);
        monitor17.metrics(iface50);
        org.apache.storm.utils.Monitor monitor54 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor54.setInterval((int) (short) -1);
        monitor54.setInterval((int) 'a');
        monitor54.setInterval((int) ' ');
        monitor54.setComponent("");
        monitor54.setTopology("hi!");
        monitor54.setInterval(10);
        org.apache.storm.utils.Monitor monitor67 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor67.setInterval((int) (short) -1);
        monitor67.setInterval((int) 'a');
        monitor67.setInterval((int) ' ');
        monitor67.setComponent("");
        monitor67.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface78 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor67.metrics(iface78);
        monitor54.metrics(iface78);
        monitor17.metrics(iface78);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface78);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor17);
        org.junit.Assert.assertNotNull(iface39);
        org.junit.Assert.assertNotNull(iface50);
        org.junit.Assert.assertNotNull(monitor54);
        org.junit.Assert.assertNotNull(monitor67);
        org.junit.Assert.assertNotNull(iface78);
    }

    @Test
    public void test1059() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1059");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setStream("");
        org.apache.storm.utils.Monitor monitor17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor17.setInterval((int) (short) -1);
        monitor17.setInterval((int) 'a');
        monitor17.setInterval((int) ' ');
        monitor17.setStream("");
        monitor17.setWatch("hi!");
        monitor17.setComponent("");
        monitor17.setWatch("");
        org.apache.storm.utils.Monitor monitor32 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor32.setInterval((int) (short) -1);
        monitor32.setInterval((int) 'a');
        monitor32.setStream("");
        monitor32.setWatch("");
        monitor32.setInterval((int) (byte) 1);
        monitor32.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface45 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor32.metrics(iface45);
        monitor17.metrics(iface45);
        monitor17.setWatch("hi!");
        org.apache.storm.generated.Nimbus.Iface iface50 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor17.metrics(iface50);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface50);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor17);
        org.junit.Assert.assertNotNull(monitor32);
        org.junit.Assert.assertNotNull(iface45);
        org.junit.Assert.assertNotNull(iface50);
    }

    @Test
    public void test1060() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1060");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (byte) 1);
        org.apache.storm.utils.Monitor monitor17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor17.setInterval((int) (short) -1);
        monitor17.setInterval((int) 'a');
        monitor17.setInterval((int) ' ');
        monitor17.setComponent("");
        monitor17.setTopology("hi!");
        monitor17.setInterval(10);
        org.apache.storm.utils.Monitor monitor30 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor30.setInterval((int) (short) -1);
        monitor30.setInterval((int) 'a');
        monitor30.setInterval((int) ' ');
        monitor30.setComponent("");
        monitor30.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface41 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor30.metrics(iface41);
        monitor17.metrics(iface41);
        monitor0.metrics(iface41);
        monitor0.setWatch("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor17);
        org.junit.Assert.assertNotNull(monitor30);
        org.junit.Assert.assertNotNull(iface41);
    }

    @Test
    public void test1061() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1061");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setInterval((int) 'a');
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1062() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1062");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
    }

    @Test
    public void test1063() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1063");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1064() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1064");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (byte) 100);
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test1065() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1065");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setInterval((int) (short) 100);
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
        org.apache.storm.utils.Monitor monitor52 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor52.setInterval((int) (short) -1);
        monitor52.setInterval((int) 'a');
        monitor52.setInterval((int) ' ');
        monitor52.setComponent("");
        monitor52.setTopology("hi!");
        monitor52.setInterval(10);
        org.apache.storm.utils.Monitor monitor65 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor65.setInterval((int) (short) -1);
        monitor65.setInterval((int) 'a');
        monitor65.setInterval((int) ' ');
        monitor65.setComponent("");
        monitor65.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface76 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor65.metrics(iface76);
        monitor52.metrics(iface76);
        monitor15.metrics(iface76);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface76);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(iface37);
        org.junit.Assert.assertNotNull(iface48);
        org.junit.Assert.assertNotNull(monitor52);
        org.junit.Assert.assertNotNull(monitor65);
        org.junit.Assert.assertNotNull(iface76);
    }

    @Test
    public void test1066() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1066");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1067() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1067");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test1068() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1068");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1069() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1069");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1070() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1070");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setInterval((int) (short) 100);
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1071() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1071");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1072() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1072");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("hi!");
        monitor0.setInterval(0);
        monitor0.setInterval(100);
        monitor0.setInterval(0);
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1073() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1073");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setInterval((int) '4');
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setInterval(0);
    }

    @Test
    public void test1074() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1074");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setInterval((-1));
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test1075() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1075");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1076() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1076");
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
        org.apache.storm.utils.Monitor monitor21 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor21.setInterval((int) (short) -1);
        monitor21.setInterval((int) 'a');
        monitor21.setStream("");
        monitor21.setWatch("");
        monitor21.setInterval((int) (byte) 1);
        monitor21.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface34 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor21.metrics(iface34);
        org.apache.storm.utils.Monitor monitor36 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor36.setInterval((int) (short) -1);
        monitor36.setInterval((int) 'a');
        monitor36.setInterval((int) ' ');
        monitor36.setComponent("");
        monitor36.setTopology("hi!");
        monitor36.setComponent("hi!");
        monitor36.setComponent("");
        monitor36.setStream("");
        monitor36.setComponent("");
        monitor36.setWatch("");
        org.apache.storm.utils.Monitor monitor57 = new org.apache.storm.utils.Monitor();
        monitor57.setStream("hi!");
        monitor57.setTopology("hi!");
        monitor57.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface64 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor57.metrics(iface64);
        monitor57.setComponent("");
        org.apache.storm.utils.Monitor monitor68 = new org.apache.storm.utils.Monitor();
        monitor68.setStream("hi!");
        monitor68.setTopology("hi!");
        monitor68.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface75 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor68.metrics(iface75);
        monitor57.metrics(iface75);
        monitor36.metrics(iface75);
        monitor21.metrics(iface75);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface75);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor21);
        org.junit.Assert.assertNotNull(iface34);
        org.junit.Assert.assertNotNull(monitor36);
        org.junit.Assert.assertNotNull(iface64);
        org.junit.Assert.assertNotNull(iface75);
    }

    @Test
    public void test1077() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1077");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setInterval((int) (byte) 0);
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test1078() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1078");
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
        monitor0.setWatch("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1079() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1079");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test1080() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1080");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1081() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1081");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setInterval((-1));
        monitor0.setInterval((int) '4');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1082() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1082");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setInterval((int) 'a');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1083() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1083");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test1084() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1084");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1085() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1085");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setStream("");
        org.apache.storm.utils.Monitor monitor21 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor21.setTopology("");
        monitor21.setComponent("hi!");
        monitor21.setStream("");
        monitor21.setComponent("");
        monitor21.setTopology("");
        monitor21.setStream("");
        monitor21.setWatch("hi!");
        monitor21.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface38 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor21.metrics(iface38);
        org.apache.storm.utils.Monitor monitor40 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor40.setInterval((int) (short) -1);
        monitor40.setInterval((int) 'a');
        monitor40.setInterval((int) ' ');
        monitor40.setComponent("");
        monitor40.setTopology("hi!");
        monitor40.setInterval(10);
        org.apache.storm.utils.Monitor monitor53 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor53.setInterval((int) (short) -1);
        monitor53.setInterval((int) 'a');
        monitor53.setInterval((int) ' ');
        monitor53.setComponent("");
        monitor53.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface64 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor53.metrics(iface64);
        monitor40.metrics(iface64);
        monitor21.metrics(iface64);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface64);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor21);
        org.junit.Assert.assertNotNull(iface38);
        org.junit.Assert.assertNotNull(monitor40);
        org.junit.Assert.assertNotNull(monitor53);
        org.junit.Assert.assertNotNull(iface64);
    }

    @Test
    public void test1086() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1086");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) -1);
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test1087() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1087");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setInterval((int) '4');
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1088() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1088");
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
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1089() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1089");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test1090() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1090");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test1091() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1091");
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
        monitor0.setComponent("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1092() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1092");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test1093() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1093");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1094() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1094");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) 10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1095() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1095");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1096() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1096");
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
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test1097() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1097");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test1098() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1098");
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
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1099() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1099");
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
        org.apache.storm.utils.Monitor monitor19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor19.setInterval((int) (short) -1);
        monitor19.setInterval((int) 'a');
        monitor19.setInterval((int) ' ');
        monitor19.setStream("");
        monitor19.setWatch("hi!");
        monitor19.setComponent("");
        monitor19.setWatch("");
        org.apache.storm.utils.Monitor monitor34 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor34.setInterval((int) (short) -1);
        monitor34.setInterval((int) 'a');
        monitor34.setStream("");
        monitor34.setWatch("");
        monitor34.setInterval((int) (byte) 1);
        monitor34.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface47 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor34.metrics(iface47);
        monitor19.metrics(iface47);
        monitor0.metrics(iface47);
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor19);
        org.junit.Assert.assertNotNull(monitor34);
        org.junit.Assert.assertNotNull(iface47);
    }

    @Test
    public void test1100() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1100");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test1101() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1101");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test1102() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1102");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor0.metrics(iface7);
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor11.setInterval((int) (short) -1);
        monitor11.setInterval((int) 'a');
        monitor11.setStream("");
        monitor11.setWatch("hi!");
        monitor11.setInterval((int) (short) 0);
        monitor11.setComponent("");
        monitor11.setStream("");
        monitor11.setInterval((int) '4');
        monitor11.setStream("");
        monitor11.setWatch("hi!");
        monitor11.setComponent("");
        monitor11.setStream("hi!");
        monitor11.setTopology("hi!");
        monitor11.setWatch("");
        org.apache.storm.utils.Monitor monitor40 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor40.setTopology("hi!");
        monitor40.setComponent("");
        org.apache.storm.utils.Monitor monitor45 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor45.setTopology("");
        monitor45.setComponent("hi!");
        monitor45.setStream("");
        monitor45.setComponent("");
        monitor45.setTopology("");
        monitor45.setStream("");
        monitor45.setWatch("hi!");
        monitor45.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface62 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor45.metrics(iface62);
        monitor40.metrics(iface62);
        monitor11.metrics(iface62);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface62);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(iface7);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(monitor40);
        org.junit.Assert.assertNotNull(monitor45);
        org.junit.Assert.assertNotNull(iface62);
    }

    @Test
    public void test1103() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1103");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) 10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1104() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1104");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) '4');
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test1105() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1105");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1106() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1106");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setInterval((int) '#');
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1107() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1107");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1108() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1108");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1109() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1109");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1110() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1110");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1111() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1111");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setStream("");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setInterval((int) ' ');
        monitor13.setStream("");
        monitor13.setWatch("hi!");
        monitor13.setComponent("");
        monitor13.setWatch("");
        org.apache.storm.utils.Monitor monitor28 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor28.setInterval((int) (short) -1);
        monitor28.setInterval((int) 'a');
        monitor28.setStream("");
        monitor28.setWatch("");
        monitor28.setInterval((int) (byte) 1);
        monitor28.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface41 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor28.metrics(iface41);
        monitor13.metrics(iface41);
        monitor13.setWatch("hi!");
        org.apache.storm.generated.Nimbus.Iface iface46 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
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
        org.junit.Assert.assertNotNull(monitor28);
        org.junit.Assert.assertNotNull(iface41);
        org.junit.Assert.assertNotNull(iface46);
    }

    @Test
    public void test1112() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1112");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test1113() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1113");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1114() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1114");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1115() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1115");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) 100);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1116() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1116");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("");
        monitor0.setInterval(10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1117() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1117");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) 10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1118() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1118");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1119() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1119");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setInterval(0);
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setTopology("hi!");
    }

    @Test
    public void test1120() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1120");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
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
    public void test1121() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1121");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1122() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1122");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) 100);
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1123() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1123");
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
        monitor0.setStream("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1124() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1124");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("");
    }

    @Test
    public void test1125() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1125");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setInterval((int) (short) 10);
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1126() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1126");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1127() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1127");
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
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1128() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1128");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test1129() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1129");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setInterval(0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1130() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1130");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1131() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1131");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        org.apache.storm.utils.Monitor monitor17 = new org.apache.storm.utils.Monitor();
        monitor17.setStream("hi!");
        monitor17.setTopology("hi!");
        monitor17.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface24 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor17.metrics(iface24);
        monitor0.metrics(iface24);
        monitor0.setInterval((int) (short) 1);
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface24);
    }

    @Test
    public void test1132() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1132");
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
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test1133() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1133");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test1134() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1134");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test1135() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1135");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) -1);
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1136() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1136");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test1137() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1137");
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
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass25 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass25);
    }

    @Test
    public void test1138() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1138");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1139() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1139");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1140() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1140");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setTopology("hi!");
        monitor13.setComponent("");
        monitor13.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor20 = new org.apache.storm.utils.Monitor();
        monitor20.setStream("hi!");
        monitor20.setTopology("hi!");
        monitor20.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface27 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor20.metrics(iface27);
        monitor13.metrics(iface27);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface27);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface27);
    }

    @Test
    public void test1141() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1141");
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
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1142() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1142");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval(100);
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test1143() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1143");
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
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test1144() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1144");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1145() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1145");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1146() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1146");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1147() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1147");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1148() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1148");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        monitor0.setInterval(0);
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1149() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1149");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1150() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1150");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1151() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1151");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setStream("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1152() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1152");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setInterval((-1));
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1153() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1153");
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
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1154() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1154");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1155() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1155");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1156() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1156");
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
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) (short) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1157() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1157");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setStream("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1158() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1158");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1159() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1159");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1160() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1160");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1161() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1161");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
    }

    @Test
    public void test1162() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1162");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setInterval((int) '4');
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
    }

    @Test
    public void test1163() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1163");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setStream("");
        org.apache.storm.utils.Monitor monitor21 = new org.apache.storm.utils.Monitor();
        monitor21.setStream("hi!");
        monitor21.setTopology("hi!");
        monitor21.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface28 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor21.metrics(iface28);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface28);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface28);
    }

    @Test
    public void test1164() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1164");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(10);
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1165() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1165");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1166() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1166");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1167() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1167");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setInterval(10);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1168() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1168");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setInterval(100);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) '#');
    }

    @Test
    public void test1169() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1169");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setWatch("");
        monitor0.setInterval(1);
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1170() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1170");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test1171() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1171");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setInterval((int) '4');
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test1172() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1172");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test1173() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1173");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor0.metrics(iface13);
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setInterval((int) ' ');
        monitor15.setComponent("");
        monitor15.setTopology("hi!");
        monitor15.setComponent("hi!");
        monitor15.setComponent("");
        monitor15.setStream("");
        monitor15.setComponent("");
        monitor15.setWatch("");
        org.apache.storm.utils.Monitor monitor36 = new org.apache.storm.utils.Monitor();
        monitor36.setStream("hi!");
        monitor36.setTopology("hi!");
        monitor36.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface43 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor36.metrics(iface43);
        monitor36.setComponent("");
        org.apache.storm.utils.Monitor monitor47 = new org.apache.storm.utils.Monitor();
        monitor47.setStream("hi!");
        monitor47.setTopology("hi!");
        monitor47.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface54 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor47.metrics(iface54);
        monitor36.metrics(iface54);
        monitor15.metrics(iface54);
        monitor0.metrics(iface54);
        monitor0.setComponent("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface13);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(iface43);
        org.junit.Assert.assertNotNull(iface54);
    }

    @Test
    public void test1174() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1174");
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
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1175() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1175");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setInterval((int) (byte) 100);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1176() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1176");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setInterval((int) (byte) 0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1177() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1177");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1178() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1178");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1179() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1179");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1180() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1180");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test1181() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1181");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (short) 10);
        org.apache.storm.utils.Monitor monitor23 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor23.setInterval((int) (short) -1);
        monitor23.setInterval((int) 'a');
        monitor23.setStream("");
        monitor23.setWatch("");
        monitor23.setInterval((int) (byte) 1);
        monitor23.setComponent("");
        monitor23.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor38 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor38.setInterval((int) (short) -1);
        monitor38.setInterval((int) 'a');
        monitor38.setInterval((int) ' ');
        monitor38.setStream("");
        monitor38.setWatch("hi!");
        monitor38.setComponent("");
        monitor38.setComponent("");
        monitor38.setWatch("");
        org.apache.storm.utils.Monitor monitor55 = new org.apache.storm.utils.Monitor();
        monitor55.setStream("hi!");
        monitor55.setTopology("hi!");
        monitor55.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface62 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor55.metrics(iface62);
        monitor38.metrics(iface62);
        monitor23.metrics(iface62);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface62);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor23);
        org.junit.Assert.assertNotNull(monitor38);
        org.junit.Assert.assertNotNull(iface62);
    }

    @Test
    public void test1182() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1182");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("hi!");
        monitor0.setInterval(0);
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) 100);
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1183() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1183");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor0.metrics(iface11);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test1184() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1184");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        monitor0.setInterval(0);
        monitor0.setStream("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1185() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1185");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1186() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1186");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setInterval((-1));
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1187() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1187");
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
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1188() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1188");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1189() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1189");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) 1);
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test1190() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1190");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.utils.Monitor monitor17 = new org.apache.storm.utils.Monitor();
        monitor17.setStream("hi!");
        monitor17.setTopology("hi!");
        monitor17.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface24 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor17.metrics(iface24);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface24);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface24);
    }

    @Test
    public void test1191() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1191");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setInterval(0);
        monitor0.setStream("hi!");
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
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor17);
        org.junit.Assert.assertNotNull(iface28);
    }

    @Test
    public void test1192() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1192");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        org.apache.storm.generated.Nimbus.Iface iface13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface13);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface13);
    }

    @Test
    public void test1193() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1193");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1194() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1194");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1195() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1195");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1196() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1196");
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
        monitor0.setInterval(1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1197() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1197");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval(10);
        org.apache.storm.utils.Monitor monitor11 = new org.apache.storm.utils.Monitor();
        monitor11.setStream("hi!");
        monitor11.setTopology("hi!");
        monitor11.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface18 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor11.metrics(iface18);
        monitor11.setWatch("hi!");
        monitor11.setComponent("");
        org.apache.storm.utils.Monitor monitor24 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor24.setInterval((int) (short) -1);
        monitor24.setInterval((int) 'a');
        monitor24.setInterval((int) ' ');
        monitor24.setStream("");
        monitor24.setWatch("hi!");
        monitor24.setComponent("");
        monitor24.setStream("hi!");
        org.apache.storm.utils.Monitor monitor39 = new org.apache.storm.utils.Monitor();
        monitor39.setStream("hi!");
        monitor39.setTopology("hi!");
        monitor39.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface46 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor39.metrics(iface46);
        monitor39.setComponent("");
        org.apache.storm.utils.Monitor monitor50 = new org.apache.storm.utils.Monitor();
        monitor50.setStream("hi!");
        monitor50.setTopology("hi!");
        monitor50.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface57 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor50.metrics(iface57);
        monitor39.metrics(iface57);
        monitor24.metrics(iface57);
        org.apache.storm.utils.Monitor monitor61 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor61.setInterval((int) (short) -1);
        monitor61.setInterval((int) 'a');
        monitor61.setInterval((int) ' ');
        monitor61.setComponent("");
        monitor61.setTopology("hi!");
        monitor61.setInterval(10);
        org.apache.storm.utils.Monitor monitor74 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor74.setInterval((int) (short) -1);
        monitor74.setInterval((int) 'a');
        monitor74.setInterval((int) ' ');
        monitor74.setComponent("");
        monitor74.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface85 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor74.metrics(iface85);
        monitor61.metrics(iface85);
        monitor24.metrics(iface85);
        monitor11.metrics(iface85);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface85);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: hi! not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface18);
        org.junit.Assert.assertNotNull(monitor24);
        org.junit.Assert.assertNotNull(iface46);
        org.junit.Assert.assertNotNull(iface57);
        org.junit.Assert.assertNotNull(monitor61);
        org.junit.Assert.assertNotNull(monitor74);
        org.junit.Assert.assertNotNull(iface85);
    }

    @Test
    public void test1198() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1198");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        org.apache.storm.generated.Nimbus.Iface iface21 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface21);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface21);
    }

    @Test
    public void test1199() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1199");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setInterval((-1));
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test1200() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1200");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setInterval((int) ' ');
        monitor15.setStream("");
        monitor15.setWatch("hi!");
        monitor15.setComponent("");
        monitor15.setComponent("");
        monitor15.setWatch("");
        org.apache.storm.utils.Monitor monitor32 = new org.apache.storm.utils.Monitor();
        monitor32.setStream("hi!");
        monitor32.setTopology("hi!");
        monitor32.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface39 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor32.metrics(iface39);
        monitor15.metrics(iface39);
        monitor0.metrics(iface39);
        monitor0.setInterval(0);
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(iface39);
    }

    @Test
    public void test1201() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1201");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1202() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1202");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        monitor0.setInterval(10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1203() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1203");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setInterval((int) '4');
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1204() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1204");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1205() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1205");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1206() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1206");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1207() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1207");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setWatch("");
        monitor0.setInterval((-1));
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1208() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1208");
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
        monitor0.setInterval((int) (byte) 0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1209() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1209");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setTopology("");
        monitor13.setComponent("hi!");
        monitor13.setStream("hi!");
        monitor13.setTopology("hi!");
        monitor13.setStream("hi!");
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
        monitor13.metrics(iface59);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface59);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(monitor26);
        org.junit.Assert.assertNotNull(iface48);
        org.junit.Assert.assertNotNull(iface59);
    }

    @Test
    public void test1210() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1210");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setInterval((int) 'a');
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test1211() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1211");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1212() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1212");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1213() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1213");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1214() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1214");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1215() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1215");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setTopology("");
        monitor13.setTopology("hi!");
        monitor13.setWatch("");
        monitor13.setTopology("hi!");
        monitor13.setComponent("");
        monitor13.setComponent("");
        monitor13.setStream("");
        org.apache.storm.utils.Monitor monitor32 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor32.setInterval((int) (short) -1);
        monitor32.setInterval((int) 'a');
        monitor32.setInterval((int) ' ');
        monitor32.setInterval((int) (byte) -1);
        monitor32.setInterval((int) (byte) 10);
        monitor32.setComponent("");
        org.apache.storm.utils.Monitor monitor45 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor45.setInterval((int) (short) -1);
        monitor45.setInterval((int) 'a');
        monitor45.setInterval((int) ' ');
        monitor45.setStream("");
        monitor45.setWatch("hi!");
        monitor45.setComponent("");
        monitor45.setComponent("");
        monitor45.setWatch("");
        org.apache.storm.utils.Monitor monitor62 = new org.apache.storm.utils.Monitor();
        monitor62.setStream("hi!");
        monitor62.setTopology("hi!");
        monitor62.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface69 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor62.metrics(iface69);
        monitor45.metrics(iface69);
        monitor32.metrics(iface69);
        monitor13.metrics(iface69);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface69);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(monitor32);
        org.junit.Assert.assertNotNull(monitor45);
        org.junit.Assert.assertNotNull(iface69);
    }

    @Test
    public void test1216() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1216");
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
        org.apache.storm.utils.Monitor monitor19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor19.setTopology("");
        monitor19.setComponent("hi!");
        monitor19.setStream("");
        monitor19.setComponent("");
        monitor19.setTopology("");
        monitor19.setStream("");
        monitor19.setWatch("hi!");
        monitor19.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface36 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor19.metrics(iface36);
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
        monitor19.metrics(iface62);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface62);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor19);
        org.junit.Assert.assertNotNull(iface36);
        org.junit.Assert.assertNotNull(monitor38);
        org.junit.Assert.assertNotNull(monitor51);
        org.junit.Assert.assertNotNull(iface62);
    }

    @Test
    public void test1217() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1217");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1218() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1218");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        org.apache.storm.utils.Monitor monitor19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor19.setTopology("hi!");
        monitor19.setComponent("");
        monitor19.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor26 = new org.apache.storm.utils.Monitor();
        monitor26.setStream("hi!");
        monitor26.setTopology("hi!");
        monitor26.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface33 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor26.metrics(iface33);
        monitor19.metrics(iface33);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface33);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor19);
        org.junit.Assert.assertNotNull(iface33);
    }

    @Test
    public void test1219() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1219");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test1220() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1220");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setTopology("");
        monitor13.setInterval((int) '4');
        monitor13.setTopology("hi!");
        monitor13.setStream("");
        monitor13.setComponent("");
        monitor13.setInterval((int) (short) 0);
        monitor13.setInterval((int) (byte) 100);
        org.apache.storm.utils.Monitor monitor32 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor32.setStream("");
        monitor32.setTopology("");
        monitor32.setTopology("hi!");
        monitor32.setInterval(100);
        monitor32.setStream("");
        monitor32.setComponent("");
        org.apache.storm.utils.Monitor monitor45 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor45.setTopology("hi!");
        monitor45.setComponent("");
        monitor45.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor52 = new org.apache.storm.utils.Monitor();
        monitor52.setStream("hi!");
        monitor52.setTopology("hi!");
        monitor52.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface59 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor52.metrics(iface59);
        monitor45.metrics(iface59);
        monitor32.metrics(iface59);
        monitor13.metrics(iface59);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface59);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(monitor32);
        org.junit.Assert.assertNotNull(monitor45);
        org.junit.Assert.assertNotNull(iface59);
    }

    @Test
    public void test1221() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1221");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1222() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1222");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setStream("");
        monitor15.setWatch("");
        monitor15.setInterval((int) (byte) 1);
        monitor15.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface28 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor15.metrics(iface28);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface28);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(iface28);
    }

    @Test
    public void test1223() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1223");
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
        monitor0.setInterval((int) (byte) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1224() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1224");
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
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1225() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1225");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setInterval(0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1226() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1226");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1227() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1227");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor0.metrics(iface11);
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test1228() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1228");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1229() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1229");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1230() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1230");
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
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setInterval(100);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1231() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1231");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1232() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1232");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setStream("");
        monitor0.setInterval(1);
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1233() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1233");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test1234() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1234");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1235() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1235");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (byte) 1);
        org.apache.storm.utils.Monitor monitor17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor17.setInterval((int) (short) -1);
        monitor17.setInterval((int) 'a');
        monitor17.setInterval((int) ' ');
        monitor17.setComponent("");
        monitor17.setTopology("hi!");
        monitor17.setInterval(10);
        org.apache.storm.utils.Monitor monitor30 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor30.setInterval((int) (short) -1);
        monitor30.setInterval((int) 'a');
        monitor30.setInterval((int) ' ');
        monitor30.setComponent("");
        monitor30.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface41 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor30.metrics(iface41);
        monitor17.metrics(iface41);
        monitor0.metrics(iface41);
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor17);
        org.junit.Assert.assertNotNull(monitor30);
        org.junit.Assert.assertNotNull(iface41);
    }

    @Test
    public void test1236() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1236");
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
        monitor0.setTopology("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1237() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1237");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
    }

    @Test
    public void test1238() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1238");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test1239() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1239");
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
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1240() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1240");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1241() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1241");
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
        monitor0.setInterval((int) (byte) 100);
        monitor0.setInterval(10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1242() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1242");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setInterval((-1));
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1243() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1243");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1244() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1244");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
    }

    @Test
    public void test1245() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1245");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1246() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1246");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) '4');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) '4');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1247() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1247");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) ' ');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1248() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1248");
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
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1249() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1249");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1250() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1250");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1251() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1251");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setInterval(0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1252() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1252");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1253() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1253");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("hi!");
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
    public void test1254() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1254");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setInterval((int) 'a');
        monitor0.setWatch("");
        org.apache.storm.utils.Monitor monitor17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor17.setInterval((int) (short) -1);
        monitor17.setInterval((int) 'a');
        monitor17.setInterval((int) ' ');
        monitor17.setComponent("");
        monitor17.setTopology("hi!");
        monitor17.setInterval(10);
        org.apache.storm.utils.Monitor monitor30 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor30.setInterval((int) (short) -1);
        monitor30.setInterval((int) 'a');
        monitor30.setTopology("");
        monitor30.setTopology("hi!");
        monitor30.setWatch("");
        monitor30.setTopology("hi!");
        monitor30.setComponent("");
        monitor30.setComponent("");
        monitor30.setStream("");
        org.apache.storm.utils.Monitor monitor49 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor49.setInterval((int) (short) -1);
        monitor49.setInterval((int) 'a');
        monitor49.setInterval((int) ' ');
        monitor49.setInterval((int) (byte) -1);
        monitor49.setInterval((int) (byte) 10);
        monitor49.setComponent("");
        org.apache.storm.utils.Monitor monitor62 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor62.setInterval((int) (short) -1);
        monitor62.setInterval((int) 'a');
        monitor62.setInterval((int) ' ');
        monitor62.setStream("");
        monitor62.setWatch("hi!");
        monitor62.setComponent("");
        monitor62.setComponent("");
        monitor62.setWatch("");
        org.apache.storm.utils.Monitor monitor79 = new org.apache.storm.utils.Monitor();
        monitor79.setStream("hi!");
        monitor79.setTopology("hi!");
        monitor79.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface86 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor79.metrics(iface86);
        monitor62.metrics(iface86);
        monitor49.metrics(iface86);
        monitor30.metrics(iface86);
        monitor17.metrics(iface86);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface86);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor17);
        org.junit.Assert.assertNotNull(monitor30);
        org.junit.Assert.assertNotNull(monitor49);
        org.junit.Assert.assertNotNull(monitor62);
        org.junit.Assert.assertNotNull(iface86);
    }

    @Test
    public void test1255() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1255");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test1256() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1256");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1257() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1257");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1258() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1258");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1259() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1259");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setInterval(100);
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
    }

    @Test
    public void test1260() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1260");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setInterval((int) (byte) 0);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1261() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1261");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1262() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1262");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
    }

    @Test
    public void test1263() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1263");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1264() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1264");
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
        monitor0.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor23 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor23.setTopology("");
        monitor23.setComponent("hi!");
        monitor23.setStream("hi!");
        monitor23.setTopology("hi!");
        monitor23.setStream("hi!");
        monitor23.setComponent("");
        org.apache.storm.utils.Monitor monitor36 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor36.setInterval((int) (short) -1);
        monitor36.setInterval((int) 'a');
        monitor36.setInterval((int) ' ');
        monitor36.setStream("");
        monitor36.setWatch("hi!");
        monitor36.setComponent("");
        monitor36.setStream("hi!");
        org.apache.storm.utils.Monitor monitor51 = new org.apache.storm.utils.Monitor();
        monitor51.setStream("hi!");
        monitor51.setTopology("hi!");
        monitor51.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface58 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor51.metrics(iface58);
        monitor51.setComponent("");
        org.apache.storm.utils.Monitor monitor62 = new org.apache.storm.utils.Monitor();
        monitor62.setStream("hi!");
        monitor62.setTopology("hi!");
        monitor62.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface69 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor62.metrics(iface69);
        monitor51.metrics(iface69);
        monitor36.metrics(iface69);
        monitor23.metrics(iface69);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface69);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor23);
        org.junit.Assert.assertNotNull(monitor36);
        org.junit.Assert.assertNotNull(iface58);
        org.junit.Assert.assertNotNull(iface69);
    }

    @Test
    public void test1265() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1265");
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
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass21 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test1266() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1266");
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
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setInterval(0);
        java.lang.Class<?> wildcardClass27 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass27);
    }

    @Test
    public void test1267() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1267");
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
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1268() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1268");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1269() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1269");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setInterval((int) '#');
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1270() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1270");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (byte) 1);
        org.apache.storm.utils.Monitor monitor17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor17.setInterval((int) (short) -1);
        monitor17.setInterval((int) 'a');
        monitor17.setInterval((int) ' ');
        monitor17.setComponent("");
        monitor17.setTopology("hi!");
        monitor17.setInterval(10);
        org.apache.storm.utils.Monitor monitor30 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor30.setInterval((int) (short) -1);
        monitor30.setInterval((int) 'a');
        monitor30.setInterval((int) ' ');
        monitor30.setComponent("");
        monitor30.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface41 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor30.metrics(iface41);
        monitor17.metrics(iface41);
        monitor0.metrics(iface41);
        monitor0.setWatch("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor17);
        org.junit.Assert.assertNotNull(monitor30);
        org.junit.Assert.assertNotNull(iface41);
    }

    @Test
    public void test1271() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1271");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1272() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1272");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setInterval((int) '#');
        monitor0.setInterval((int) (byte) 0);
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test1273() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1273");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test1274() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1274");
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
        monitor15.setWatch("hi!");
        org.apache.storm.generated.Nimbus.Iface iface48 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor15.metrics(iface48);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface48);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(monitor30);
        org.junit.Assert.assertNotNull(iface43);
        org.junit.Assert.assertNotNull(iface48);
    }

    @Test
    public void test1275() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1275");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setStream("");
        monitor15.setWatch("");
        monitor15.setInterval((int) ' ');
        monitor15.setStream("");
        monitor15.setWatch("");
        monitor15.setComponent("");
        org.apache.storm.utils.Monitor monitor32 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor32.setInterval((int) (short) -1);
        monitor32.setInterval((int) 'a');
        monitor32.setInterval((int) ' ');
        monitor32.setComponent("");
        monitor32.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface43 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor32.metrics(iface43);
        monitor15.metrics(iface43);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface43);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(monitor32);
        org.junit.Assert.assertNotNull(iface43);
    }

    @Test
    public void test1276() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1276");
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
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1277() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1277");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 1);
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1278() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1278");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1279() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1279");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1280() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1280");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 1);
        org.apache.storm.utils.Monitor monitor11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor11.setInterval((int) (short) -1);
        monitor11.setInterval((int) 'a');
        monitor11.setInterval((int) ' ');
        monitor11.setComponent("");
        monitor11.setTopology("hi!");
        monitor11.setInterval(10);
        org.apache.storm.utils.Monitor monitor24 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor24.setInterval((int) (short) -1);
        monitor24.setInterval((int) 'a');
        monitor24.setTopology("");
        monitor24.setTopology("hi!");
        monitor24.setWatch("");
        monitor24.setTopology("hi!");
        monitor24.setComponent("");
        monitor24.setComponent("");
        monitor24.setStream("");
        org.apache.storm.utils.Monitor monitor43 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor43.setInterval((int) (short) -1);
        monitor43.setInterval((int) 'a');
        monitor43.setInterval((int) ' ');
        monitor43.setInterval((int) (byte) -1);
        monitor43.setInterval((int) (byte) 10);
        monitor43.setComponent("");
        org.apache.storm.utils.Monitor monitor56 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor56.setInterval((int) (short) -1);
        monitor56.setInterval((int) 'a');
        monitor56.setInterval((int) ' ');
        monitor56.setStream("");
        monitor56.setWatch("hi!");
        monitor56.setComponent("");
        monitor56.setComponent("");
        monitor56.setWatch("");
        org.apache.storm.utils.Monitor monitor73 = new org.apache.storm.utils.Monitor();
        monitor73.setStream("hi!");
        monitor73.setTopology("hi!");
        monitor73.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface80 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor73.metrics(iface80);
        monitor56.metrics(iface80);
        monitor43.metrics(iface80);
        monitor24.metrics(iface80);
        monitor11.metrics(iface80);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface80);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: hi! not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(monitor24);
        org.junit.Assert.assertNotNull(monitor43);
        org.junit.Assert.assertNotNull(monitor56);
        org.junit.Assert.assertNotNull(iface80);
    }

    @Test
    public void test1281() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1281");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setInterval(100);
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setStream("");
        monitor15.setWatch("");
        monitor15.setInterval((int) (byte) 1);
        monitor15.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface28 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor15.metrics(iface28);
        org.apache.storm.utils.Monitor monitor30 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor30.setInterval((int) (short) -1);
        monitor30.setInterval((int) 'a');
        monitor30.setInterval((int) ' ');
        monitor30.setInterval((int) (byte) -1);
        monitor30.setInterval((int) (byte) 10);
        monitor30.setComponent("");
        org.apache.storm.utils.Monitor monitor43 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor43.setInterval((int) (short) -1);
        monitor43.setInterval((int) 'a');
        monitor43.setInterval((int) ' ');
        monitor43.setStream("");
        monitor43.setWatch("hi!");
        monitor43.setComponent("");
        monitor43.setComponent("");
        monitor43.setWatch("");
        org.apache.storm.utils.Monitor monitor60 = new org.apache.storm.utils.Monitor();
        monitor60.setStream("hi!");
        monitor60.setTopology("hi!");
        monitor60.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface67 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor60.metrics(iface67);
        monitor43.metrics(iface67);
        monitor30.metrics(iface67);
        monitor15.metrics(iface67);
        monitor0.metrics(iface67);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(iface28);
        org.junit.Assert.assertNotNull(monitor30);
        org.junit.Assert.assertNotNull(monitor43);
        org.junit.Assert.assertNotNull(iface67);
    }

    @Test
    public void test1282() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1282");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setTopology("");
        monitor13.setComponent("hi!");
        monitor13.setStream("");
        monitor13.setComponent("");
        monitor13.setTopology("");
        monitor13.setStream("");
        monitor13.setWatch("hi!");
        monitor13.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface30 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor13.metrics(iface30);
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
        monitor13.metrics(iface56);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface56);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface30);
        org.junit.Assert.assertNotNull(monitor32);
        org.junit.Assert.assertNotNull(monitor45);
        org.junit.Assert.assertNotNull(iface56);
    }

    @Test
    public void test1283() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1283");
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
        monitor0.setInterval(100);
        monitor0.setInterval((int) '#');
        org.apache.storm.utils.Monitor monitor25 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor25.setStream("");
        monitor25.setTopology("");
        monitor25.setTopology("hi!");
        monitor25.setInterval(100);
        monitor25.setStream("");
        monitor25.setComponent("");
        org.apache.storm.utils.Monitor monitor38 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor38.setTopology("hi!");
        monitor38.setComponent("");
        monitor38.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor45 = new org.apache.storm.utils.Monitor();
        monitor45.setStream("hi!");
        monitor45.setTopology("hi!");
        monitor45.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface52 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor45.metrics(iface52);
        monitor38.metrics(iface52);
        monitor25.metrics(iface52);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface52);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor25);
        org.junit.Assert.assertNotNull(monitor38);
        org.junit.Assert.assertNotNull(iface52);
    }

    @Test
    public void test1284() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1284");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 1);
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1285() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1285");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1286() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1286");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setInterval(0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1287() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1287");
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
        monitor0.metrics(iface56);
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor19);
        org.junit.Assert.assertNotNull(monitor32);
        org.junit.Assert.assertNotNull(iface56);
    }

    @Test
    public void test1288() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1288");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 1);
        monitor0.setTopology("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1289() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1289");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) '4');
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test1290() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1290");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1291() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1291");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setComponent("");
        monitor0.setTopology("");
    }

    @Test
    public void test1292() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1292");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setStream("");
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
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface24);
        org.junit.Assert.assertNotNull(iface35);
    }

    @Test
    public void test1293() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1293");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 0);
        org.apache.storm.utils.Monitor monitor19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor19.setTopology("");
        monitor19.setComponent("hi!");
        monitor19.setStream("");
        monitor19.setComponent("");
        monitor19.setTopology("");
        monitor19.setStream("");
        monitor19.setWatch("hi!");
        monitor19.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface36 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor19.metrics(iface36);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface36);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor19);
        org.junit.Assert.assertNotNull(iface36);
    }

    @Test
    public void test1294() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1294");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval(10);
        monitor0.setInterval((int) (short) 1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1295() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1295");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setComponent("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1296() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1296");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        org.apache.storm.utils.Monitor monitor13 = new org.apache.storm.utils.Monitor();
        monitor13.setStream("hi!");
        monitor13.setTopology("hi!");
        monitor13.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface20 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor13.metrics(iface20);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface20);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface20);
    }

    @Test
    public void test1297() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1297");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor0.metrics(iface13);
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setInterval((int) ' ');
        monitor15.setComponent("");
        monitor15.setTopology("hi!");
        monitor15.setComponent("hi!");
        monitor15.setComponent("");
        monitor15.setStream("");
        monitor15.setComponent("");
        monitor15.setWatch("");
        org.apache.storm.utils.Monitor monitor36 = new org.apache.storm.utils.Monitor();
        monitor36.setStream("hi!");
        monitor36.setTopology("hi!");
        monitor36.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface43 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor36.metrics(iface43);
        monitor36.setComponent("");
        org.apache.storm.utils.Monitor monitor47 = new org.apache.storm.utils.Monitor();
        monitor47.setStream("hi!");
        monitor47.setTopology("hi!");
        monitor47.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface54 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor47.metrics(iface54);
        monitor36.metrics(iface54);
        monitor15.metrics(iface54);
        monitor0.metrics(iface54);
        monitor0.setComponent("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface13);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(iface43);
        org.junit.Assert.assertNotNull(iface54);
    }

    @Test
    public void test1298() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1298");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1299() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1299");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setInterval(0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1300() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1300");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        org.apache.storm.utils.Monitor monitor19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor19.setInterval((int) (short) -1);
        monitor19.setInterval((int) 'a');
        monitor19.setTopology("");
        monitor19.setInterval((int) '4');
        monitor19.setTopology("hi!");
        monitor19.setStream("");
        monitor19.setComponent("");
        monitor19.setInterval((int) (short) 0);
        monitor19.setInterval((int) (byte) 100);
        org.apache.storm.utils.Monitor monitor38 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor38.setStream("");
        monitor38.setTopology("");
        monitor38.setTopology("hi!");
        monitor38.setInterval(100);
        monitor38.setStream("");
        monitor38.setComponent("");
        org.apache.storm.utils.Monitor monitor51 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor51.setTopology("hi!");
        monitor51.setComponent("");
        monitor51.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor58 = new org.apache.storm.utils.Monitor();
        monitor58.setStream("hi!");
        monitor58.setTopology("hi!");
        monitor58.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface65 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor58.metrics(iface65);
        monitor51.metrics(iface65);
        monitor38.metrics(iface65);
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
        org.junit.Assert.assertNotNull(monitor38);
        org.junit.Assert.assertNotNull(monitor51);
        org.junit.Assert.assertNotNull(iface65);
    }

    @Test
    public void test1301() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1301");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1302() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1302");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 1);
    }

    @Test
    public void test1303() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1303");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1304() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1304");
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
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1305() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1305");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        org.apache.storm.utils.Monitor monitor11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor11.setTopology("");
        monitor11.setComponent("hi!");
        monitor11.setStream("");
        monitor11.setComponent("");
        monitor11.setTopology("");
        monitor11.setStream("");
        monitor11.setWatch("hi!");
        monitor11.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface28 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor11.metrics(iface28);
        org.apache.storm.utils.Monitor monitor30 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor30.setInterval((int) (short) -1);
        monitor30.setInterval((int) 'a');
        monitor30.setInterval((int) ' ');
        monitor30.setComponent("");
        monitor30.setTopology("hi!");
        monitor30.setInterval(10);
        org.apache.storm.utils.Monitor monitor43 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor43.setInterval((int) (short) -1);
        monitor43.setInterval((int) 'a');
        monitor43.setInterval((int) ' ');
        monitor43.setComponent("");
        monitor43.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface54 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor43.metrics(iface54);
        monitor30.metrics(iface54);
        monitor11.metrics(iface54);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface54);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(iface28);
        org.junit.Assert.assertNotNull(monitor30);
        org.junit.Assert.assertNotNull(monitor43);
        org.junit.Assert.assertNotNull(iface54);
    }

    @Test
    public void test1306() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1306");
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
        monitor0.setInterval((int) (byte) -1);
        monitor0.setStream("");
        monitor0.setInterval((int) ' ');
        monitor0.setInterval(0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1307() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1307");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) 10);
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
        org.apache.storm.utils.Monitor monitor30 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor30.setInterval((int) (short) -1);
        monitor30.setInterval((int) 'a');
        monitor30.setStream("");
        monitor30.setWatch("hi!");
        monitor30.setInterval((int) (short) 0);
        monitor30.setComponent("");
        monitor30.setStream("");
        monitor30.setInterval((int) '4');
        monitor30.setStream("");
        monitor30.setWatch("hi!");
        monitor30.setComponent("");
        monitor30.setStream("hi!");
        monitor30.setTopology("hi!");
        monitor30.setWatch("");
        org.apache.storm.utils.Monitor monitor59 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor59.setTopology("hi!");
        monitor59.setComponent("");
        org.apache.storm.utils.Monitor monitor64 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor64.setTopology("");
        monitor64.setComponent("hi!");
        monitor64.setStream("");
        monitor64.setComponent("");
        monitor64.setTopology("");
        monitor64.setStream("");
        monitor64.setWatch("hi!");
        monitor64.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface81 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor64.metrics(iface81);
        monitor59.metrics(iface81);
        monitor30.metrics(iface81);
        monitor9.metrics(iface81);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface81);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor9);
        org.junit.Assert.assertNotNull(monitor30);
        org.junit.Assert.assertNotNull(monitor59);
        org.junit.Assert.assertNotNull(monitor64);
        org.junit.Assert.assertNotNull(iface81);
    }

    @Test
    public void test1308() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1308");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test1309() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1309");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test1310() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1310");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1311() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1311");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setComponent("");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setTopology("hi!");
        monitor13.setComponent("");
        monitor13.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor20 = new org.apache.storm.utils.Monitor();
        monitor20.setStream("hi!");
        monitor20.setTopology("hi!");
        monitor20.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface27 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor20.metrics(iface27);
        monitor13.metrics(iface27);
        monitor0.metrics(iface27);
        monitor0.setInterval(100);
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface27);
    }

    @Test
    public void test1312() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1312");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1313() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1313");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setInterval(10);
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1314() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1314");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setTopology("");
        monitor0.setTopology("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) ' ');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1315() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1315");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setStream("");
    }

    @Test
    public void test1316() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1316");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setInterval((int) (short) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1317() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1317");
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
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1318() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1318");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setInterval(100);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
    }

    @Test
    public void test1319() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1319");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setInterval((int) (byte) 1);
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test1320() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1320");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1321() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1321");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setInterval(0);
        monitor0.setStream("");
        monitor0.setComponent("");
        org.apache.storm.utils.Monitor monitor19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor19.setInterval((int) (short) -1);
        monitor19.setInterval((int) 'a');
        monitor19.setInterval((int) ' ');
        monitor19.setComponent("");
        monitor19.setTopology("hi!");
        monitor19.setInterval(10);
        org.apache.storm.utils.Monitor monitor32 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor32.setInterval((int) (short) -1);
        monitor32.setInterval((int) 'a');
        monitor32.setTopology("");
        monitor32.setTopology("hi!");
        monitor32.setWatch("");
        monitor32.setTopology("hi!");
        monitor32.setComponent("");
        monitor32.setComponent("");
        monitor32.setStream("");
        org.apache.storm.utils.Monitor monitor51 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor51.setInterval((int) (short) -1);
        monitor51.setInterval((int) 'a');
        monitor51.setInterval((int) ' ');
        monitor51.setInterval((int) (byte) -1);
        monitor51.setInterval((int) (byte) 10);
        monitor51.setComponent("");
        org.apache.storm.utils.Monitor monitor64 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor64.setInterval((int) (short) -1);
        monitor64.setInterval((int) 'a');
        monitor64.setInterval((int) ' ');
        monitor64.setStream("");
        monitor64.setWatch("hi!");
        monitor64.setComponent("");
        monitor64.setComponent("");
        monitor64.setWatch("");
        org.apache.storm.utils.Monitor monitor81 = new org.apache.storm.utils.Monitor();
        monitor81.setStream("hi!");
        monitor81.setTopology("hi!");
        monitor81.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface88 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor81.metrics(iface88);
        monitor64.metrics(iface88);
        monitor51.metrics(iface88);
        monitor32.metrics(iface88);
        monitor19.metrics(iface88);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface88);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor19);
        org.junit.Assert.assertNotNull(monitor32);
        org.junit.Assert.assertNotNull(monitor51);
        org.junit.Assert.assertNotNull(monitor64);
        org.junit.Assert.assertNotNull(iface88);
    }

    @Test
    public void test1322() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1322");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) 1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1323() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1323");
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
        monitor0.setStream("hi!");
        org.apache.storm.utils.Monitor monitor21 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor21.setInterval((int) (short) -1);
        monitor21.setInterval((int) 'a');
        monitor21.setInterval((int) ' ');
        monitor21.setStream("");
        monitor21.setWatch("hi!");
        monitor21.setComponent("");
        monitor21.setStream("hi!");
        org.apache.storm.utils.Monitor monitor36 = new org.apache.storm.utils.Monitor();
        monitor36.setStream("hi!");
        monitor36.setTopology("hi!");
        monitor36.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface43 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor36.metrics(iface43);
        monitor36.setComponent("");
        org.apache.storm.utils.Monitor monitor47 = new org.apache.storm.utils.Monitor();
        monitor47.setStream("hi!");
        monitor47.setTopology("hi!");
        monitor47.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface54 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor47.metrics(iface54);
        monitor36.metrics(iface54);
        monitor21.metrics(iface54);
        org.apache.storm.utils.Monitor monitor58 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor58.setInterval((int) (short) -1);
        monitor58.setInterval((int) 'a');
        monitor58.setInterval((int) ' ');
        monitor58.setComponent("");
        monitor58.setTopology("hi!");
        monitor58.setInterval(10);
        org.apache.storm.utils.Monitor monitor71 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor71.setInterval((int) (short) -1);
        monitor71.setInterval((int) 'a');
        monitor71.setInterval((int) ' ');
        monitor71.setComponent("");
        monitor71.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface82 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor71.metrics(iface82);
        monitor58.metrics(iface82);
        monitor21.metrics(iface82);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface82);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor21);
        org.junit.Assert.assertNotNull(iface43);
        org.junit.Assert.assertNotNull(iface54);
        org.junit.Assert.assertNotNull(monitor58);
        org.junit.Assert.assertNotNull(monitor71);
        org.junit.Assert.assertNotNull(iface82);
    }

    @Test
    public void test1324() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1324");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor0.metrics(iface7);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setStream("");
        monitor0.setInterval((int) '#');
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(iface7);
    }

    @Test
    public void test1325() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1325");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) '#');
        monitor0.setInterval((int) 'a');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1326() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1326");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test1327() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1327");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1328() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1328");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1329() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1329");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1330() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1330");
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
        monitor0.setInterval((int) (byte) 1);
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test1331() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1331");
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
        java.lang.Class<?> wildcardClass65 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface22);
        org.junit.Assert.assertNotNull(iface33);
        org.junit.Assert.assertNotNull(monitor37);
        org.junit.Assert.assertNotNull(monitor50);
        org.junit.Assert.assertNotNull(iface61);
        org.junit.Assert.assertNotNull(wildcardClass65);
    }

    @Test
    public void test1332() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1332");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test1333() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1333");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1334() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1334");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1335() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1335");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setInterval((int) (byte) -1);
        monitor0.setInterval((int) (byte) 10);
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) 1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1336() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1336");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
    }

    @Test
    public void test1337() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1337");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
    }

    @Test
    public void test1338() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1338");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1339() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1339");
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
        monitor0.setInterval((int) (byte) -1);
        monitor0.setStream("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1340() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1340");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1341() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1341");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) '#');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1342() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1342");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setTopology("hi!");
    }

    @Test
    public void test1343() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1343");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1344() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1344");
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
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1345() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1345");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) (byte) 10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1346() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1346");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setTopology("");
        monitor0.setInterval(0);
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1347() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1347");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1348() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1348");
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
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1349() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1349");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1350() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1350");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1351() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1351");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor0.metrics(iface7);
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
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
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(iface7);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(iface29);
    }

    @Test
    public void test1352() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1352");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setWatch("hi!");
        monitor0.setInterval(0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1353() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1353");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor0.metrics(iface11);
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test1354() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1354");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) -1);
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test1355() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1355");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (short) 1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1356() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1356");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setStream("");
        monitor13.setWatch("hi!");
        monitor13.setInterval((int) (short) 0);
        monitor13.setComponent("");
        monitor13.setStream("");
        monitor13.setInterval((int) '4');
        monitor13.setStream("");
        monitor13.setWatch("hi!");
        org.apache.storm.utils.Monitor monitor34 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor34.setInterval((int) (short) -1);
        monitor34.setInterval((int) 'a');
        monitor34.setStream("");
        monitor34.setWatch("hi!");
        monitor34.setInterval((int) (short) 0);
        monitor34.setComponent("");
        monitor34.setStream("");
        monitor34.setInterval((int) '4');
        monitor34.setStream("");
        monitor34.setWatch("hi!");
        monitor34.setComponent("");
        monitor34.setStream("hi!");
        monitor34.setTopology("hi!");
        monitor34.setWatch("");
        org.apache.storm.utils.Monitor monitor63 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor63.setTopology("hi!");
        monitor63.setComponent("");
        org.apache.storm.utils.Monitor monitor68 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor68.setTopology("");
        monitor68.setComponent("hi!");
        monitor68.setStream("");
        monitor68.setComponent("");
        monitor68.setTopology("");
        monitor68.setStream("");
        monitor68.setWatch("hi!");
        monitor68.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface85 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor68.metrics(iface85);
        monitor63.metrics(iface85);
        monitor34.metrics(iface85);
        monitor13.metrics(iface85);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface85);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(monitor34);
        org.junit.Assert.assertNotNull(monitor63);
        org.junit.Assert.assertNotNull(monitor68);
        org.junit.Assert.assertNotNull(iface85);
    }

    @Test
    public void test1357() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1357");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1358() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1358");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        org.apache.storm.utils.Monitor monitor13 = new org.apache.storm.utils.Monitor();
        monitor13.setStream("hi!");
        monitor13.setTopology("hi!");
        monitor13.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface20 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor13.metrics(iface20);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface20);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface20);
    }

    @Test
    public void test1359() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1359");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1360() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1360");
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
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1361() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1361");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setInterval(0);
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1362() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1362");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (byte) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1363() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1363");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setInterval((int) ' ');
        monitor15.setStream("");
        monitor15.setWatch("hi!");
        monitor15.setComponent("");
        monitor15.setComponent("");
        monitor15.setWatch("");
        org.apache.storm.utils.Monitor monitor32 = new org.apache.storm.utils.Monitor();
        monitor32.setStream("hi!");
        monitor32.setTopology("hi!");
        monitor32.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface39 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor32.metrics(iface39);
        monitor15.metrics(iface39);
        monitor0.metrics(iface39);
        monitor0.setInterval(0);
        java.lang.Class<?> wildcardClass45 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(iface39);
        org.junit.Assert.assertNotNull(wildcardClass45);
    }

    @Test
    public void test1364() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1364");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1365() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1365");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setInterval((int) '4');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1366() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1366");
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
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test1367() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1367");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) '#');
        monitor0.setComponent("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1368() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1368");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setInterval((int) ' ');
        monitor15.setStream("");
        monitor15.setWatch("hi!");
        monitor15.setComponent("");
        monitor15.setComponent("");
        monitor15.setWatch("");
        org.apache.storm.utils.Monitor monitor32 = new org.apache.storm.utils.Monitor();
        monitor32.setStream("hi!");
        monitor32.setTopology("hi!");
        monitor32.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface39 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor32.metrics(iface39);
        monitor15.metrics(iface39);
        monitor0.metrics(iface39);
        java.lang.Class<?> wildcardClass43 = iface39.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(iface39);
        org.junit.Assert.assertNotNull(wildcardClass43);
    }

    @Test
    public void test1369() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1369");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1370() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1370");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setInterval((int) (byte) -1);
        monitor0.setInterval((int) (short) -1);
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1371() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1371");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1372() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1372");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1373() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1373");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1374() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1374");
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
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1375() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1375");
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
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1376() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1376");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1377() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1377");
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
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test1378() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1378");
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
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test1379() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1379");
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
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test1380() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1380");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1381() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1381");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setComponent("");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setStream("");
        monitor13.setWatch("");
        monitor13.setInterval((int) (byte) 1);
        monitor13.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface26 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor13.metrics(iface26);
        monitor0.metrics(iface26);
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface26);
    }

    @Test
    public void test1382() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1382");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1383() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1383");
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
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test1384() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1384");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setTopology("");
        monitor0.setInterval((-1));
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1385() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1385");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1386() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1386");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
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
    public void test1387() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1387");
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
    public void test1388() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1388");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (short) 100);
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1389() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1389");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1390() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1390");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1391() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1391");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1392() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1392");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1393() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1393");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1394() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1394");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        org.apache.storm.generated.Nimbus.Iface iface17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface17);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface17);
    }

    @Test
    public void test1395() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1395");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1396() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1396");
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
        monitor0.setTopology("");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1397() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1397");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1398() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1398");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setTopology("");
        monitor0.setInterval(0);
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1399() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1399");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test1400() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1400");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) '#');
        monitor0.setInterval(100);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1401() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1401");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test1402() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1402");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setInterval((int) 'a');
        monitor0.setWatch("");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1403() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1403");
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
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) '4');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1404() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1404");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1405() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1405");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1406() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1406");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        java.lang.Class<?> wildcardClass5 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test1407() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1407");
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
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test1408() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1408");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        org.apache.storm.utils.Monitor monitor17 = new org.apache.storm.utils.Monitor();
        monitor17.setStream("hi!");
        monitor17.setTopology("hi!");
        monitor17.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface24 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor17.metrics(iface24);
        monitor0.metrics(iface24);
        java.lang.Class<?> wildcardClass27 = iface24.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface24);
        org.junit.Assert.assertNotNull(wildcardClass27);
    }

    @Test
    public void test1409() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1409");
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
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor17);
        org.junit.Assert.assertNotNull(iface34);
    }

    @Test
    public void test1410() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1410");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1411() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1411");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1412() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1412");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setWatch("hi!");
        org.apache.storm.utils.Monitor monitor17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor17.setInterval((int) (short) -1);
        monitor17.setInterval((int) 'a');
        monitor17.setStream("");
        monitor17.setWatch("");
        monitor17.setInterval((int) (byte) 1);
        monitor17.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface30 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor17.metrics(iface30);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface30);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor17);
        org.junit.Assert.assertNotNull(iface30);
    }

    @Test
    public void test1413() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1413");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
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
        monitor15.setWatch("hi!");
        org.apache.storm.generated.Nimbus.Iface iface48 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor15.metrics(iface48);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface48);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(monitor30);
        org.junit.Assert.assertNotNull(iface43);
        org.junit.Assert.assertNotNull(iface48);
    }

    @Test
    public void test1414() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1414");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
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
    public void test1415() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1415");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) 0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1416() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1416");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) 0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1417() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1417");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1418() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1418");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setInterval((int) (byte) 1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1419() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1419");
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
        monitor0.setInterval((int) (short) 10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1420() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1420");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1421() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1421");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) '4');
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1422() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1422");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1423() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1423");
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
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1424() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1424");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1425() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1425");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1426() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1426");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setStream("");
        monitor13.setTopology("");
        monitor13.setTopology("hi!");
        monitor13.setTopology("");
        monitor13.setComponent("");
        monitor13.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor26 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor26.setStream("");
        monitor26.setTopology("");
        monitor26.setTopology("hi!");
        monitor26.setInterval(100);
        monitor26.setStream("");
        monitor26.setComponent("");
        org.apache.storm.utils.Monitor monitor39 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor39.setTopology("hi!");
        monitor39.setComponent("");
        monitor39.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor46 = new org.apache.storm.utils.Monitor();
        monitor46.setStream("hi!");
        monitor46.setTopology("hi!");
        monitor46.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface53 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor46.metrics(iface53);
        monitor39.metrics(iface53);
        monitor26.metrics(iface53);
        monitor13.metrics(iface53);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface53);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(monitor26);
        org.junit.Assert.assertNotNull(monitor39);
        org.junit.Assert.assertNotNull(iface53);
    }

    @Test
    public void test1427() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1427");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1428() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1428");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setWatch("");
        monitor0.setInterval(0);
        monitor0.setInterval((int) (short) 0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1429() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1429");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1430() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1430");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) '#');
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1431() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1431");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval(1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1432() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1432");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (short) 100);
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test1433() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1433");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (byte) 100);
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1434() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1434");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor0.metrics(iface13);
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setInterval((int) ' ');
        monitor15.setComponent("");
        monitor15.setTopology("hi!");
        monitor15.setComponent("hi!");
        monitor15.setComponent("");
        monitor15.setStream("");
        monitor15.setComponent("");
        monitor15.setWatch("");
        org.apache.storm.utils.Monitor monitor36 = new org.apache.storm.utils.Monitor();
        monitor36.setStream("hi!");
        monitor36.setTopology("hi!");
        monitor36.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface43 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor36.metrics(iface43);
        monitor36.setComponent("");
        org.apache.storm.utils.Monitor monitor47 = new org.apache.storm.utils.Monitor();
        monitor47.setStream("hi!");
        monitor47.setTopology("hi!");
        monitor47.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface54 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor47.metrics(iface54);
        monitor36.metrics(iface54);
        monitor15.metrics(iface54);
        monitor0.metrics(iface54);
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface13);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(iface43);
        org.junit.Assert.assertNotNull(iface54);
    }

    @Test
    public void test1435() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1435");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor9 = new org.apache.storm.utils.Monitor();
        monitor9.setStream("hi!");
        monitor9.setTopology("hi!");
        monitor9.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface16 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor9.metrics(iface16);
        monitor9.setComponent("");
        org.apache.storm.utils.Monitor monitor20 = new org.apache.storm.utils.Monitor();
        monitor20.setStream("hi!");
        monitor20.setTopology("hi!");
        monitor20.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface27 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor20.metrics(iface27);
        monitor9.metrics(iface27);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface27);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface16);
        org.junit.Assert.assertNotNull(iface27);
    }

    @Test
    public void test1436() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1436");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 1);
        monitor0.setInterval((int) (short) 100);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1437() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1437");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1438() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1438");
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
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1439() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1439");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval(1);
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1440() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1440");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1441() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1441");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setWatch("");
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
    public void test1442() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1442");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1443() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1443");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 100);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1444() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1444");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) 0);
    }

    @Test
    public void test1445() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1445");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setInterval(100);
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setWatch("");
    }

    @Test
    public void test1446() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1446");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setInterval(10);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1447() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1447");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1448() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1448");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) 0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1449() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1449");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1450() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1450");
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
        monitor0.setComponent("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1451() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1451");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 100);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1452() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1452");
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
        monitor0.setWatch("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1453() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1453");
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
        monitor0.setInterval((int) '4');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1454() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1454");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        monitor0.setInterval(0);
        monitor0.setInterval((int) (short) 10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1455() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1455");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1456() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1456");
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
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1457() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1457");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setStream("");
        monitor0.setInterval(100);
        java.lang.Class<?> wildcardClass21 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test1458() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1458");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setInterval((-1));
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1459() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1459");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1460() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1460");
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
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1461() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1461");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("");
        monitor0.setInterval(0);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1462() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1462");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setStream("");
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
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface33);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface22);
        org.junit.Assert.assertNotNull(iface33);
    }

    @Test
    public void test1463() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1463");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setStream("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1464() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1464");
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
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1465() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1465");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setInterval((int) '4');
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setInterval((int) ' ');
        monitor15.setStream("");
        monitor15.setWatch("hi!");
        monitor15.setComponent("");
        monitor15.setComponent("");
        monitor15.setWatch("");
        org.apache.storm.utils.Monitor monitor32 = new org.apache.storm.utils.Monitor();
        monitor32.setStream("hi!");
        monitor32.setTopology("hi!");
        monitor32.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface39 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor32.metrics(iface39);
        monitor15.metrics(iface39);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface39);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(iface39);
    }

    @Test
    public void test1466() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1466");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setInterval(10);
        monitor0.setWatch("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1467() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1467");
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
        monitor0.setInterval((int) 'a');
        org.apache.storm.utils.Monitor monitor23 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor23.setInterval((int) (short) -1);
        monitor23.setInterval((int) 'a');
        monitor23.setInterval((int) ' ');
        monitor23.setStream("");
        monitor23.setWatch("hi!");
        monitor23.setComponent("");
        monitor23.setWatch("");
        org.apache.storm.utils.Monitor monitor38 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor38.setInterval((int) (short) -1);
        monitor38.setInterval((int) 'a');
        monitor38.setStream("");
        monitor38.setWatch("");
        monitor38.setInterval((int) (byte) 1);
        monitor38.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface51 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor38.metrics(iface51);
        monitor23.metrics(iface51);
        monitor0.metrics(iface51);
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor23);
        org.junit.Assert.assertNotNull(monitor38);
        org.junit.Assert.assertNotNull(iface51);
    }

    @Test
    public void test1468() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1468");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test1469() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1469");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test1470() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1470");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setWatch("");
        org.apache.storm.utils.Monitor monitor7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor7.setInterval((int) (short) -1);
        monitor7.setInterval((int) 'a');
        monitor7.setInterval((int) ' ');
        monitor7.setComponent("");
        monitor7.setTopology("hi!");
        monitor7.setInterval(10);
        org.apache.storm.utils.Monitor monitor20 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor20.setInterval((int) (short) -1);
        monitor20.setInterval((int) 'a');
        monitor20.setInterval((int) ' ');
        monitor20.setComponent("");
        monitor20.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface31 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor20.metrics(iface31);
        monitor7.metrics(iface31);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface31);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor7);
        org.junit.Assert.assertNotNull(monitor20);
        org.junit.Assert.assertNotNull(iface31);
    }

    @Test
    public void test1471() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest2.test1471");
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
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }
}

