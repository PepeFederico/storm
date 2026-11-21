package org.apache.storm.utils.staticmocking.randoop;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RegressionTest0 {

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
    public void test0001() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0001");
        org.apache.storm.generated.Nimbus.Iface iface0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        java.lang.Class<?> wildcardClass1 = iface0.getClass();
        org.junit.Assert.assertNotNull(iface0);
        org.junit.Assert.assertNotNull(wildcardClass1);
    }

    @Test
    public void test0002() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0002");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface7);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface7);
    }

    @Test
    public void test0003() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0003");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        java.lang.Class<?> wildcardClass3 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0004() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0004");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0005() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0005");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        java.lang.Class<?> wildcardClass5 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0006() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0006");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0007() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0007");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0008() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0008");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        org.apache.storm.generated.Nimbus.Iface iface1 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface1);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(iface1);
    }

    @Test
    public void test0009() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0009");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setWatch("hi!");
        org.apache.storm.generated.Nimbus.Iface iface13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
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
    public void test0010() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0010");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        org.apache.storm.generated.Nimbus.Iface iface9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface9);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface9);
    }

    @Test
    public void test0011() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0011");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0012() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0012");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        org.apache.storm.generated.Nimbus.Iface iface5 = null;
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface5);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0013() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0013");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0014() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0014");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("");
        org.apache.storm.generated.Nimbus.Iface iface9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface9);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface9);
    }

    @Test
    public void test0015() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0015");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0016() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0016");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0017() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0017");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setStream("");
        org.apache.storm.generated.Nimbus.Iface iface15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
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
    public void test0018() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0018");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        org.apache.storm.generated.Nimbus.Iface iface5 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface5);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface5);
    }

    @Test
    public void test0019() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0019");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface9);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: comp-1 not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface9);
    }

    @Test
    public void test0020() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0020");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0021() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0021");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0022() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0022");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0023() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0023");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0024() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0024");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface7);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream: hi! not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface7);
    }

    @Test
    public void test0025() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0025");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0026() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0026");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0027() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0027");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0028() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0028");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0029() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0029");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0030() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0030");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0031() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0031");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        org.apache.storm.generated.Nimbus.Iface iface9 = null;
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface9);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0032() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0032");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0033() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0033");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        org.apache.storm.generated.Nimbus.Iface iface9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface9);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface9);
    }

    @Test
    public void test0034() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0034");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface13);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: hi! not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface13);
    }

    @Test
    public void test0035() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0035");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        org.apache.storm.generated.Nimbus.Iface iface3 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface3);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface3);
    }

    @Test
    public void test0036() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0036");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0037() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0037");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0038() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0038");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0039() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0039");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0040() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0040");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0041() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0041");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setComponent("");
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
    public void test0042() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0042");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass3 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0043() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0043");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        org.apache.storm.generated.Nimbus.Iface iface1 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface1);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: comp-1 not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface1);
    }

    @Test
    public void test0044() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0044");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0045() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0045");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        java.lang.Class<?> wildcardClass5 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0046() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0046");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        org.apache.storm.generated.Nimbus.Iface iface5 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface5);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface5);
    }

    @Test
    public void test0047() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0047");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0048() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0048");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0049() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0049");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface7);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: comp-1 not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface7);
    }

    @Test
    public void test0050() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0050");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 0);
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0051() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0051");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0052() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0052");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0053() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0053");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        org.apache.storm.generated.Nimbus.Iface iface5 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface5);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: comp-1 not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface5);
    }

    @Test
    public void test0054() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0054");
        java.lang.Object obj0 = new java.lang.Object();
        java.lang.Class<?> wildcardClass1 = obj0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass1);
    }

    @Test
    public void test0055() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0055");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0056() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0056");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        org.apache.storm.generated.Nimbus.Iface iface5 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface5);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: comp-1 not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface5);
    }

    @Test
    public void test0057() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0057");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0058() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0058");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setStream("");
        org.apache.storm.generated.Nimbus.Iface iface13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
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
    public void test0059() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0059");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("");
        org.apache.storm.generated.Nimbus.Iface iface9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface9);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface9);
    }

    @Test
    public void test0060() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0060");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface13);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface13);
    }

    @Test
    public void test0061() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0061");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0062() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0062");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
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
    public void test0063() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0063");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0064() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0064");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0065() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0065");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream: default not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0066() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0066");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0067() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0067");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0068() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0068");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0069() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0069");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setWatch("");
        org.apache.storm.generated.Nimbus.Iface iface5 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface5);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(iface5);
    }

    @Test
    public void test0070() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0070");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0071() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0071");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        monitor0.setWatch("");
        org.apache.storm.generated.Nimbus.Iface iface13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface13);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface13);
    }

    @Test
    public void test0072() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0072");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setInterval(100);
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface7);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(iface7);
    }

    @Test
    public void test0073() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0073");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0074() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0074");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setInterval((int) (short) 100);
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0075() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0075");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface7);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(iface7);
    }

    @Test
    public void test0076() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0076");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        org.apache.storm.generated.Nimbus.Iface iface13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface13);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface13);
    }

    @Test
    public void test0077() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0077");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        org.apache.storm.generated.Nimbus.Iface iface5 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface5);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: comp-1 not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface5);
    }

    @Test
    public void test0078() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0078");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0079() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0079");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0080() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0080");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("hi!");
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
    public void test0081() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0081");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        org.apache.storm.generated.Nimbus.Iface iface1 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface1);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream: default not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface1);
    }

    @Test
    public void test0082() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0082");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setInterval((int) ' ');
        monitor13.setComponent("");
        monitor13.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface24 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor13.metrics(iface24);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface24);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: hi! not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface24);
    }

    @Test
    public void test0083() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0083");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0084() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0084");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setWatch("");
        org.apache.storm.generated.Nimbus.Iface iface15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
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
    public void test0085() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0085");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
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
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(iface22);
    }

    @Test
    public void test0086() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0086");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) -1);
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
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(iface22);
    }

    @Test
    public void test0087() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0087");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: comp-1 not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0088() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0088");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        java.lang.Class<?> wildcardClass1 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass1);
    }

    @Test
    public void test0089() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0089");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        org.apache.storm.generated.Nimbus.Iface iface13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
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
    public void test0090() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0090");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        org.apache.storm.generated.Nimbus.Iface iface9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface9);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface9);
    }

    @Test
    public void test0091() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0091");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0092() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0092");
        org.apache.storm.generated.Nimbus.Iface iface0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        java.lang.Class<?> wildcardClass1 = iface0.getClass();
        org.junit.Assert.assertNotNull(iface0);
        org.junit.Assert.assertNotNull(wildcardClass1);
    }

    @Test
    public void test0093() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0093");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0094() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0094");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setInterval((int) ' ');
        monitor13.setComponent("");
        monitor13.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface24 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor13.metrics(iface24);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface24);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface24);
    }

    @Test
    public void test0095() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0095");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0096() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0096");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0097() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0097");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0098() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0098");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        org.apache.storm.generated.Nimbus.Iface iface9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface9);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface9);
    }

    @Test
    public void test0099() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0099");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
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
    public void test0100() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0100");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setWatch("");
        org.apache.storm.generated.Nimbus.Iface iface15 = null;
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface15);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0101() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0101");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setInterval(100);
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0102() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0102");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0103() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0103");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setInterval(0);
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0104() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0104");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0105() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0105");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0106() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0106");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0107() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0107");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        org.apache.storm.generated.Nimbus.Iface iface3 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface3);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface3);
    }

    @Test
    public void test0108() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0108");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass5 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0109() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0109");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0110() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0110");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = null;
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface7);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.generated.Nimbus$Iface.getTopologyInfoByNameWithOpts(String, org.apache.storm.generated.GetInfoOptions)\" because \"client\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0111() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0111");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0112() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0112");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0113() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0113");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0114() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0114");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0115() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0115");
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
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0116() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0116");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        org.apache.storm.generated.Nimbus.Iface iface17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
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
    public void test0117() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0117");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface3 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface3);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(iface3);
    }

    @Test
    public void test0118() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0118");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0119() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0119");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
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
    public void test0120() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0120");
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
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor17);
        org.junit.Assert.assertNotNull(iface28);
    }

    @Test
    public void test0121() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0121");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0122() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0122");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0123() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0123");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass3 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0124() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0124");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
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
    public void test0125() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0125");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0126() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0126");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0127() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0127");
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
    public void test0128() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0128");
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
    public void test0129() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0129");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setWatch("");
        org.apache.storm.generated.Nimbus.Iface iface15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
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
    public void test0130() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0130");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setInterval((int) (short) 100);
        monitor0.setInterval(1);
        org.apache.storm.generated.Nimbus.Iface iface17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
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
    public void test0131() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0131");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setInterval((int) (byte) 0);
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0132() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0132");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0133() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0133");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
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
    public void test0134() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0134");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0135() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0135");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) 0);
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0136() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0136");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        org.apache.storm.utils.Monitor monitor15 = new org.apache.storm.utils.Monitor();
        monitor15.setStream("hi!");
        monitor15.setTopology("hi!");
        monitor15.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface22 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor15.metrics(iface22);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface22);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface22);
    }

    @Test
    public void test0137() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0137");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0138() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0138");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setInterval((int) (byte) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0139() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0139");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0140() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0140");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (byte) 10);
        org.apache.storm.generated.Nimbus.Iface iface15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface15);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface15);
    }

    @Test
    public void test0141() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0141");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0142() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0142");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0143() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0143");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setInterval(10);
        monitor0.setWatch("");
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
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface22);
    }

    @Test
    public void test0144() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0144");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
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
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface18);
    }

    @Test
    public void test0145() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0145");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface5 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface5);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(iface5);
    }

    @Test
    public void test0146() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0146");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0147() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0147");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) 0);
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setInterval((int) ' ');
        monitor13.setComponent("");
        monitor13.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface24 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor13.metrics(iface24);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface24);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface24);
    }

    @Test
    public void test0148() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0148");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass3 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0149() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0149");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0150() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0150");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0151() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0151");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setInterval((int) 'a');
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0152() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0152");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 10);
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0153() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0153");
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
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0154() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0154");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0155() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0155");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (short) 10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0156() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0156");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
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
    public void test0157() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0157");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
    }

    @Test
    public void test0158() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0158");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0159() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0159");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0160() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0160");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0161() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0161");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0162() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0162");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        org.apache.storm.utils.Monitor monitor3 = new org.apache.storm.utils.Monitor();
        monitor3.setStream("hi!");
        monitor3.setTopology("hi!");
        monitor3.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface10 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor3.metrics(iface10);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface10);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream: default not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface10);
    }

    @Test
    public void test0163() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0163");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0164() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0164");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0165() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0165");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0166() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0166");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0167() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0167");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0168() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0168");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        org.apache.storm.generated.Nimbus.Iface iface9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface9);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface9);
    }

    @Test
    public void test0169() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0169");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setWatch("hi!");
        org.apache.storm.generated.Nimbus.Iface iface9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface9);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface9);
    }

    @Test
    public void test0170() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0170");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setInterval((-1));
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0171() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0171");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0172() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0172");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
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
    public void test0173() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0173");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface13);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface13);
    }

    @Test
    public void test0174() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0174");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setInterval((int) '4');
        java.lang.Class<?> wildcardClass3 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass3);
    }

    @Test
    public void test0175() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0175");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setInterval((int) '4');
        monitor0.setWatch("");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0176() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0176");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setWatch("");
        org.apache.storm.generated.Nimbus.Iface iface5 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface5);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface5);
    }

    @Test
    public void test0177() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0177");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) '#');
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0178() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0178");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0179() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0179");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        org.apache.storm.utils.Monitor monitor1 = new org.apache.storm.utils.Monitor();
        monitor1.setStream("hi!");
        monitor1.setTopology("hi!");
        monitor1.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface8 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor1.metrics(iface8);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface8);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream: default not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface8);
    }

    @Test
    public void test0180() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0180");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        org.apache.storm.generated.Nimbus.Iface iface7 = null;
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface7);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0181() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0181");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setInterval((-1));
        org.apache.storm.generated.Nimbus.Iface iface13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
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
    public void test0182() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0182");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 10);
        org.apache.storm.utils.Monitor monitor15 = new org.apache.storm.utils.Monitor();
        monitor15.setStream("hi!");
        monitor15.setTopology("hi!");
        monitor15.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface22 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor15.metrics(iface22);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface22);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface22);
    }

    @Test
    public void test0183() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0183");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
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
    public void test0184() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0184");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
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
    public void test0185() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0185");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
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
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(iface26);
    }

    @Test
    public void test0186() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0186");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("");
    }

    @Test
    public void test0187() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0187");
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
        org.apache.storm.generated.Nimbus.Iface iface19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface19);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface19);
    }

    @Test
    public void test0188() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0188");
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
        java.lang.Class<?> wildcardClass21 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test0189() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0189");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) 0);
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0190() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0190");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        org.apache.storm.utils.Monitor monitor5 = new org.apache.storm.utils.Monitor();
        monitor5.setStream("hi!");
        monitor5.setTopology("hi!");
        monitor5.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface12 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor5.metrics(iface12);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface12);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface12);
    }

    @Test
    public void test0191() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0191");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) 10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0192() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0192");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
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
    public void test0193() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0193");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        org.apache.storm.generated.Nimbus.Iface iface9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface9);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface9);
    }

    @Test
    public void test0194() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0194");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0195() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0195");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("hi!");
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
    public void test0196() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0196");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0197() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0197");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = null;
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.generated.Nimbus$Iface.getTopologyInfoByNameWithOpts(String, org.apache.storm.generated.GetInfoOptions)\" because \"client\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0198() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0198");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 10);
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0199() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0199");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0200() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0200");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setStream("");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0201() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0201");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 1);
        monitor0.setTopology("");
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
    public void test0202() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0202");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setInterval(100);
        monitor0.setWatch("hi!");
        org.apache.storm.generated.Nimbus.Iface iface9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface9);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(iface9);
    }

    @Test
    public void test0203() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0203");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0204() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0204");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface7);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface7);
    }

    @Test
    public void test0205() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0205");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0206() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0206");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0207() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0207");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setInterval(0);
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0208() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0208");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0209() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0209");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0210() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0210");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setInterval((int) ' ');
        monitor13.setComponent("");
        monitor13.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface24 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor13.metrics(iface24);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface24);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface24);
    }

    @Test
    public void test0211() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0211");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0212() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0212");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0213() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0213");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0214() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0214");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0215() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0215");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor0.metrics(iface11);
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0216() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0216");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0217() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0217");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        org.apache.storm.generated.Nimbus.Iface iface15 = null;
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface15);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0218() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0218");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
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
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(iface22);
    }

    @Test
    public void test0219() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0219");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setWatch("");
        org.apache.storm.utils.Monitor monitor3 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor3.setInterval((int) (short) -1);
        monitor3.setInterval((int) 'a');
        monitor3.setInterval((int) ' ');
        monitor3.setComponent("");
        monitor3.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface14 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor3.metrics(iface14);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface14);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor3);
        org.junit.Assert.assertNotNull(iface14);
    }

    @Test
    public void test0220() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0220");
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
        org.apache.storm.generated.Nimbus.Iface iface19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
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
    public void test0221() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0221");
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
        org.apache.storm.utils.Monitor monitor19 = new org.apache.storm.utils.Monitor();
        monitor19.setStream("hi!");
        monitor19.setTopology("hi!");
        monitor19.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface26 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor19.metrics(iface26);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface26);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface26);
    }

    @Test
    public void test0222() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0222");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0223() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0223");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0224() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0224");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0225() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0225");
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
        java.lang.Class<?> wildcardClass21 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test0226() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0226");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(10);
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0227() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0227");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor0.metrics(iface7);
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(iface7);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0228() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0228");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        org.apache.storm.generated.Nimbus.Iface iface15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface15);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface15);
    }

    @Test
    public void test0229() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0229");
        org.apache.storm.generated.Nimbus.Iface iface0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        java.lang.Class<?> wildcardClass1 = iface0.getClass();
        org.junit.Assert.assertNotNull(iface0);
        org.junit.Assert.assertNotNull(wildcardClass1);
    }

    @Test
    public void test0230() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0230");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 10);
        java.lang.Class<?> wildcardClass5 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0231() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0231");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0232() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0232");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        org.apache.storm.utils.Monitor monitor9 = new org.apache.storm.utils.Monitor();
        monitor9.setStream("hi!");
        monitor9.setTopology("hi!");
        monitor9.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface16 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor9.metrics(iface16);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface16);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface16);
    }

    @Test
    public void test0233() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0233");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) (short) 100);
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
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
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(iface22);
    }

    @Test
    public void test0234() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0234");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0235() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0235");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        monitor0.setInterval((int) (byte) 10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0236() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0236");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0237() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0237");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0238() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0238");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setTopology("");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0239() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0239");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0240() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0240");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0241() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0241");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0242() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0242");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass5 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0243() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0243");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0244() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0244");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setInterval((int) ' ');
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0245() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0245");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setInterval((int) ' ');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0246() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0246");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0247() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0247");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setInterval((int) (byte) -1);
        monitor0.setInterval((int) (short) 0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0248() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0248");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setStream("");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0249() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0249");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0250() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0250");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0251() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0251");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0252() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0252");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        org.apache.storm.utils.Monitor monitor3 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor3.setInterval((int) (short) -1);
        monitor3.setInterval((int) 'a');
        monitor3.setInterval((int) ' ');
        monitor3.setComponent("");
        monitor3.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface14 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor3.metrics(iface14);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface14);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor3);
        org.junit.Assert.assertNotNull(iface14);
    }

    @Test
    public void test0253() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0253");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setStream("");
        org.apache.storm.generated.Nimbus.Iface iface13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface13);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface13);
    }

    @Test
    public void test0254() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0254");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setWatch("");
    }

    @Test
    public void test0255() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0255");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setInterval(0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0256() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0256");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) '4');
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0257() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0257");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0258() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0258");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0259() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0259");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setWatch("");
        org.apache.storm.generated.Nimbus.Iface iface17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface17);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface17);
    }

    @Test
    public void test0260() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0260");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
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
    public void test0261() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0261");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
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
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: hi! not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(monitor26);
        org.junit.Assert.assertNotNull(iface37);
    }

    @Test
    public void test0262() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0262");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0263() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0263");
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
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0264() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0264");
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
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0265() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0265");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0266() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0266");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0267() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0267");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) 1);
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0268() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0268");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 100);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0269() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0269");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
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
    public void test0270() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0270");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setInterval((int) 'a');
        monitor0.setWatch("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0271() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0271");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass5 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0272() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0272");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setInterval((int) (byte) -1);
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0273() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0273");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setComponent("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0274() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0274");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) '4');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0275() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0275");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0276() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0276");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setInterval((-1));
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0277() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0277");
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
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0278() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0278");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
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
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface41);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor17);
        org.junit.Assert.assertNotNull(monitor30);
        org.junit.Assert.assertNotNull(iface41);
    }

    @Test
    public void test0279() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0279");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0280() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0280");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0281() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0281");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setInterval((int) (short) 100);
        monitor0.setInterval((int) (byte) 100);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0282() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0282");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor11.setInterval((int) (short) -1);
        monitor11.setInterval((int) 'a');
        monitor11.setInterval((int) ' ');
        monitor11.setStream("");
        monitor11.setWatch("hi!");
        monitor11.setComponent("");
        monitor11.setWatch("");
        org.apache.storm.utils.Monitor monitor26 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor26.setInterval((int) (short) -1);
        monitor26.setInterval((int) 'a');
        monitor26.setStream("");
        monitor26.setWatch("");
        monitor26.setInterval((int) (byte) 1);
        monitor26.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface39 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor26.metrics(iface39);
        monitor11.metrics(iface39);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface39);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(monitor26);
        org.junit.Assert.assertNotNull(iface39);
    }

    @Test
    public void test0283() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0283");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0284() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0284");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0285() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0285");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (byte) 100);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0286() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0286");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setComponent("hi!");
        org.apache.storm.generated.Nimbus.Iface iface13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface13);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: hi! not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface13);
    }

    @Test
    public void test0287() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0287");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        org.apache.storm.utils.Monitor monitor5 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor5.setInterval((int) (short) -1);
        monitor5.setInterval((int) 'a');
        monitor5.setStream("");
        monitor5.setWatch("");
        monitor5.setInterval((int) (byte) 1);
        monitor5.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface18 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor5.metrics(iface18);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface18);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: hi! not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor5);
        org.junit.Assert.assertNotNull(iface18);
    }

    @Test
    public void test0288() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0288");
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
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0289() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0289");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setInterval((int) (byte) 0);
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0290() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0290");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0291() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0291");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0292() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0292");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0293() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0293");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
    }

    @Test
    public void test0294() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0294");
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
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0295() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0295");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0296() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0296");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass5 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0297() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0297");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setInterval(1);
        org.apache.storm.generated.Nimbus.Iface iface19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface19);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface19);
    }

    @Test
    public void test0298() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0298");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setInterval(100);
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0299() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0299");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0300() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0300");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        org.apache.storm.generated.Nimbus.Iface iface15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
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
    public void test0301() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0301");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval(1);
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0302() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0302");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0303() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0303");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        monitor0.setStream("");
        monitor0.setInterval(10);
    }

    @Test
    public void test0304() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0304");
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
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0305() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0305");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0306() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0306");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0307() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0307");
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
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface24);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface24);
    }

    @Test
    public void test0308() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0308");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0309() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0309");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0310() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0310");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass5 = monitor0.getClass();
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0311() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0311");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0312() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0312");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setInterval((int) (byte) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0313() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0313");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("");
        monitor0.setInterval((int) '#');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0314() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0314");
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
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0315() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0315");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) 0);
        monitor0.setInterval((int) (byte) 100);
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0316() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0316");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("hi!");
        org.apache.storm.generated.Nimbus.Iface iface17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface17);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface17);
    }

    @Test
    public void test0317() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0317");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        org.apache.storm.utils.Monitor monitor11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor11.setInterval((int) (short) -1);
        monitor11.setInterval((int) 'a');
        monitor11.setInterval((int) ' ');
        monitor11.setStream("");
        monitor11.setWatch("hi!");
        monitor11.setComponent("");
        monitor11.setWatch("");
        org.apache.storm.utils.Monitor monitor26 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor26.setInterval((int) (short) -1);
        monitor26.setInterval((int) 'a');
        monitor26.setStream("");
        monitor26.setWatch("");
        monitor26.setInterval((int) (byte) 1);
        monitor26.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface39 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor26.metrics(iface39);
        monitor11.metrics(iface39);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface39);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(monitor26);
        org.junit.Assert.assertNotNull(iface39);
    }

    @Test
    public void test0318() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0318");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setStream("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0319() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0319");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) '4');
        monitor0.setInterval((int) (byte) -1);
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
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface47);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor19);
        org.junit.Assert.assertNotNull(monitor34);
        org.junit.Assert.assertNotNull(iface47);
    }

    @Test
    public void test0320() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0320");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0321() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0321");
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
        java.lang.Class<?> wildcardClass21 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test0322() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0322");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setTopology("");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0323() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0323");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0324() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0324");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        org.apache.storm.utils.Monitor monitor7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor7.setInterval((int) (short) -1);
        monitor7.setInterval((int) 'a');
        monitor7.setInterval((int) ' ');
        monitor7.setComponent("");
        monitor7.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface18 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor7.metrics(iface18);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface18);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor7);
        org.junit.Assert.assertNotNull(iface18);
    }

    @Test
    public void test0325() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0325");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0326() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0326");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0327() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0327");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass5 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass5);
    }

    @Test
    public void test0328() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0328");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setInterval((-1));
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor17 = new org.apache.storm.utils.Monitor();
        monitor17.setStream("hi!");
        monitor17.setTopology("hi!");
        monitor17.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface24 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor17.metrics(iface24);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface24);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface24);
    }

    @Test
    public void test0329() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0329");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0330() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0330");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0331() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0331");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setInterval((int) ' ');
        monitor15.setComponent("");
        monitor15.setTopology("hi!");
        monitor15.setInterval(10);
        org.apache.storm.utils.Monitor monitor28 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor28.setInterval((int) (short) -1);
        monitor28.setInterval((int) 'a');
        monitor28.setInterval((int) ' ');
        monitor28.setComponent("");
        monitor28.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface39 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor28.metrics(iface39);
        monitor15.metrics(iface39);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface39);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(monitor28);
        org.junit.Assert.assertNotNull(iface39);
    }

    @Test
    public void test0332() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0332");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setInterval((int) ' ');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0333() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0333");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) -1);
        monitor0.setStream("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0334() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0334");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0335() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0335");
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
        org.apache.storm.generated.Nimbus.Iface iface19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface19);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface19);
    }

    @Test
    public void test0336() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0336");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 10);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0337() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0337");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0338() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0338");
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
        monitor21.setInterval((int) ' ');
        monitor21.setComponent("");
        monitor21.setTopology("hi!");
        monitor21.setInterval(10);
        org.apache.storm.utils.Monitor monitor34 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor34.setInterval((int) (short) -1);
        monitor34.setInterval((int) 'a');
        monitor34.setInterval((int) ' ');
        monitor34.setComponent("");
        monitor34.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface45 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor34.metrics(iface45);
        monitor21.metrics(iface45);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface45);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor21);
        org.junit.Assert.assertNotNull(monitor34);
        org.junit.Assert.assertNotNull(iface45);
    }

    @Test
    public void test0339() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0339");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setWatch("");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0340() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0340");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0341() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0341");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setInterval((int) '#');
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0342() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0342");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setInterval((int) ' ');
        monitor13.setStream("");
        monitor13.setWatch("hi!");
        monitor13.setComponent("");
        monitor13.setWatch("");
        monitor13.setComponent("");
        org.apache.storm.utils.Monitor monitor30 = new org.apache.storm.utils.Monitor();
        monitor30.setStream("hi!");
        monitor30.setTopology("hi!");
        monitor30.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface37 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor30.metrics(iface37);
        monitor13.metrics(iface37);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface37);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface37);
    }

    @Test
    public void test0343() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0343");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0344() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0344");
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
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0345() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0345");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 1);
        monitor0.setInterval((int) (byte) 10);
    }

    @Test
    public void test0346() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0346");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        org.apache.storm.utils.Monitor monitor15 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor15.setInterval((int) (short) -1);
        monitor15.setInterval((int) 'a');
        monitor15.setInterval((int) ' ');
        monitor15.setComponent("");
        monitor15.setTopology("hi!");
        monitor15.setInterval(10);
        org.apache.storm.utils.Monitor monitor28 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor28.setInterval((int) (short) -1);
        monitor28.setInterval((int) 'a');
        monitor28.setInterval((int) ' ');
        monitor28.setComponent("");
        monitor28.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface39 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor28.metrics(iface39);
        monitor15.metrics(iface39);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface39);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(monitor28);
        org.junit.Assert.assertNotNull(iface39);
    }

    @Test
    public void test0347() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0347");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setInterval(100);
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
    public void test0348() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0348");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setInterval(0);
        monitor0.setWatch("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0349() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0349");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("");
        monitor0.setStream("");
        monitor0.setInterval((int) (short) 0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0350() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0350");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        org.apache.storm.utils.Monitor monitor3 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor3.setInterval((int) (short) -1);
        monitor3.setInterval((int) 'a');
        monitor3.setInterval((int) ' ');
        monitor3.setStream("");
        monitor3.setWatch("hi!");
        monitor3.setComponent("");
        monitor3.setWatch("");
        monitor3.setComponent("");
        org.apache.storm.utils.Monitor monitor20 = new org.apache.storm.utils.Monitor();
        monitor20.setStream("hi!");
        monitor20.setTopology("hi!");
        monitor20.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface27 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor20.metrics(iface27);
        monitor3.metrics(iface27);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface27);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor3);
        org.junit.Assert.assertNotNull(iface27);
    }

    @Test
    public void test0351() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0351");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor0.metrics(iface7);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(iface7);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0352() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0352");
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
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0353() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0353");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0354() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0354");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setComponent("");
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
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface22);
    }

    @Test
    public void test0355() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0355");
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
        java.lang.Class<?> wildcardClass21 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test0356() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0356");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setInterval((-1));
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setInterval((int) ' ');
        monitor13.setStream("");
        monitor13.setWatch("hi!");
        monitor13.setComponent("");
        monitor13.setWatch("");
        monitor13.setComponent("");
        org.apache.storm.utils.Monitor monitor30 = new org.apache.storm.utils.Monitor();
        monitor30.setStream("hi!");
        monitor30.setTopology("hi!");
        monitor30.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface37 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor30.metrics(iface37);
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
        org.junit.Assert.assertNotNull(iface37);
    }

    @Test
    public void test0357() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0357");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setInterval((int) (byte) -1);
        monitor0.setInterval((int) (short) -1);
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
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(iface22);
    }

    @Test
    public void test0358() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0358");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 1);
        org.apache.storm.utils.Monitor monitor11 = new org.apache.storm.utils.Monitor();
        monitor11.setStream("hi!");
        monitor11.setTopology("hi!");
        monitor11.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface18 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor11.metrics(iface18);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface18);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: hi! not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface18);
    }

    @Test
    public void test0359() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0359");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0360() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0360");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setInterval((int) (byte) -1);
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0361() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0361");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setWatch("");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface7);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface7);
    }

    @Test
    public void test0362() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0362");
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
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface24);
    }

    @Test
    public void test0363() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0363");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0364() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0364");
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
        monitor19.setStream("");
        monitor19.setWatch("");
        monitor19.setInterval((int) (byte) 1);
        monitor19.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface32 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor19.metrics(iface32);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface32);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor19);
        org.junit.Assert.assertNotNull(iface32);
    }

    @Test
    public void test0365() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0365");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0366() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0366");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setInterval((int) (byte) -1);
        monitor0.setInterval((int) (short) -1);
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
        monitor24.setInterval((int) ' ');
        monitor24.setComponent("");
        monitor24.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface35 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor24.metrics(iface35);
        monitor11.metrics(iface35);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface35);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(monitor24);
        org.junit.Assert.assertNotNull(iface35);
    }

    @Test
    public void test0367() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0367");
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
        org.apache.storm.utils.Monitor monitor19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor19.setInterval((int) (short) -1);
        monitor19.setInterval((int) 'a');
        monitor19.setStream("");
        monitor19.setWatch("");
        monitor19.setInterval((int) (byte) 1);
        monitor19.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface32 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor19.metrics(iface32);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface32);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor19);
        org.junit.Assert.assertNotNull(iface32);
    }

    @Test
    public void test0368() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0368");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (byte) 10);
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0369() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0369");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (byte) 1);
        monitor0.setComponent("hi!");
        org.apache.storm.utils.Monitor monitor13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor13.setInterval((int) (short) -1);
        monitor13.setInterval((int) 'a');
        monitor13.setInterval((int) ' ');
        monitor13.setComponent("");
        monitor13.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface24 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor13.metrics(iface24);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface24);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface24);
    }

    @Test
    public void test0370() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0370");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        monitor0.setInterval(10);
        monitor0.setComponent("hi!");
        org.apache.storm.utils.Monitor monitor19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor19.setInterval((int) (short) -1);
        monitor19.setInterval((int) 'a');
        monitor19.setStream("");
        monitor19.setWatch("");
        monitor19.setInterval((int) (byte) 1);
        monitor19.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface32 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor19.metrics(iface32);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface32);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor19);
        org.junit.Assert.assertNotNull(iface32);
    }

    @Test
    public void test0371() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0371");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setInterval(100);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0372() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0372");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setInterval((int) (short) 0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0373() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0373");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setWatch("");
        org.apache.storm.utils.Monitor monitor5 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor5.setInterval((int) (short) -1);
        monitor5.setInterval((int) 'a');
        monitor5.setInterval((int) ' ');
        monitor5.setComponent("");
        monitor5.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface16 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor5.metrics(iface16);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface16);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor5);
        org.junit.Assert.assertNotNull(iface16);
    }

    @Test
    public void test0374() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0374");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0375() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0375");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 1);
        org.apache.storm.generated.Nimbus.Iface iface11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface11);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0376() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0376");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("hi!");
        monitor0.setInterval(0);
        monitor0.setInterval(100);
        org.apache.storm.utils.Monitor monitor11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor11.setInterval((int) (short) -1);
        monitor11.setInterval((int) 'a');
        monitor11.setInterval((int) ' ');
        monitor11.setStream("");
        monitor11.setWatch("hi!");
        monitor11.setComponent("");
        monitor11.setWatch("");
        org.apache.storm.utils.Monitor monitor26 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor26.setInterval((int) (short) -1);
        monitor26.setInterval((int) 'a');
        monitor26.setStream("");
        monitor26.setWatch("");
        monitor26.setInterval((int) (byte) 1);
        monitor26.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface39 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor26.metrics(iface39);
        monitor11.metrics(iface39);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface39);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: comp-1 not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(monitor26);
        org.junit.Assert.assertNotNull(iface39);
    }

    @Test
    public void test0377() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0377");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        monitor0.setInterval(0);
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0378() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0378");
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
        org.apache.storm.generated.Nimbus.Iface iface19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
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
    public void test0379() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0379");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0380() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0380");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0381() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0381");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0382() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0382");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        org.apache.storm.generated.Nimbus.Iface iface17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface17);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface17);
    }

    @Test
    public void test0383() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0383");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setInterval((-1));
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0384() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0384");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("");
        monitor0.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface17);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface17);
    }

    @Test
    public void test0385() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0385");
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
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0386() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0386");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setWatch("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0387() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0387");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setInterval((int) (byte) -1);
        org.apache.storm.utils.Monitor monitor9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor9.setInterval((int) (short) -1);
        monitor9.setInterval((int) 'a');
        monitor9.setInterval((int) ' ');
        monitor9.setComponent("");
        monitor9.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface20 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor9.metrics(iface20);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface20);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor9);
        org.junit.Assert.assertNotNull(iface20);
    }

    @Test
    public void test0388() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0388");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.utils.Monitor monitor5 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor5.setInterval((int) (short) -1);
        monitor5.setInterval((int) 'a');
        monitor5.setInterval((int) ' ');
        monitor5.setComponent("");
        monitor5.setTopology("hi!");
        monitor5.setInterval(10);
        org.apache.storm.utils.Monitor monitor18 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor18.setInterval((int) (short) -1);
        monitor18.setInterval((int) 'a');
        monitor18.setInterval((int) ' ');
        monitor18.setComponent("");
        monitor18.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface29 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor18.metrics(iface29);
        monitor5.metrics(iface29);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface29);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor5);
        org.junit.Assert.assertNotNull(monitor18);
        org.junit.Assert.assertNotNull(iface29);
    }

    @Test
    public void test0389() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0389");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setTopology("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0390() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0390");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((int) (short) 0);
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) ' ');
        org.apache.storm.utils.Monitor monitor15 = new org.apache.storm.utils.Monitor();
        monitor15.setStream("hi!");
        monitor15.setTopology("hi!");
        monitor15.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface22 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor15.metrics(iface22);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface22);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream: default not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface22);
    }

    @Test
    public void test0391() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0391");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0392() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0392");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setInterval((int) (short) 10);
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0393() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0393");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0394() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0394");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface9 = null;
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface9);
            org.junit.Assert.fail("Expected exception of type java.lang.NullPointerException; message: Cannot invoke \"org.apache.storm.generated.Nimbus$Iface.getTopologyInfoByNameWithOpts(String, org.apache.storm.generated.GetInfoOptions)\" because \"client\" is null");
        } catch (java.lang.NullPointerException e) {
            // Expected exception.
        }
    }

    @Test
    public void test0395() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0395");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0396() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0396");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        monitor0.setInterval(0);
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0397() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0397");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0398() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0398");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setInterval(100);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0399() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0399");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setInterval(0);
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
    public void test0400() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0400");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0401() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0401");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("hi!");
        monitor0.setTopology("");
        monitor0.setInterval(0);
        monitor0.setInterval((int) (short) -1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0402() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0402");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0403() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0403");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("");
        monitor0.setStream("hi!");
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
    public void test0404() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0404");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setInterval((int) '4');
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0405() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0405");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0406() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0406");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0407() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0407");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setStream("hi!");
        org.apache.storm.utils.Monitor monitor11 = new org.apache.storm.utils.Monitor();
        monitor11.setStream("hi!");
        monitor11.setTopology("hi!");
        monitor11.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface18 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor11.metrics(iface18);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface18);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface18);
    }

    @Test
    public void test0408() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0408");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0409() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0409");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface7);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface7);
    }

    @Test
    public void test0410() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0410");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
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
    public void test0411() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0411");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("");
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
    public void test0412() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0412");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((int) (short) 0);
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0413() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0413");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0414() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0414");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0415() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0415");
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
        java.lang.Class<?> wildcardClass21 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test0416() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0416");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) '4');
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0417() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0417");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
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
    public void test0418() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0418");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
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
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface26);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface26);
    }

    @Test
    public void test0419() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0419");
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
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0420() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0420");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0421() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0421");
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
        java.lang.Class<?> wildcardClass23 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass23);
    }

    @Test
    public void test0422() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0422");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0423() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0423");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setInterval((int) '#');
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0424() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0424");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setInterval((int) (short) 100);
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0425() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0425");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setInterval((int) ' ');
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
        org.apache.storm.generated.Nimbus.Iface iface17 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
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
    public void test0426() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0426");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0427() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0427");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface13 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface13);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface13);
    }

    @Test
    public void test0428() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0428");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setComponent("hi!");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0429() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0429");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0430() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0430");
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
        java.lang.Class<?> wildcardClass21 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test0431() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0431");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor0.metrics(iface7);
        monitor0.setWatch("hi!");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(iface7);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0432() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0432");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setStream("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0433() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0433");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass7 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass7);
    }

    @Test
    public void test0434() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0434");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0435() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0435");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setWatch("");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0436() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0436");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setStream("hi!");
        monitor0.setInterval(1);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0437() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0437");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 1);
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
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface28);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: hi! not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(iface28);
    }

    @Test
    public void test0438() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0438");
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
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(iface7);
        org.junit.Assert.assertNotNull(monitor15);
        org.junit.Assert.assertNotNull(monitor30);
        org.junit.Assert.assertNotNull(iface43);
    }

    @Test
    public void test0439() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0439");
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
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass21 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test0440() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0440");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0441() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0441");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval(1);
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0442() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0442");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setComponent("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0443() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0443");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setInterval((int) ' ');
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0444() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0444");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor0.metrics(iface7);
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setInterval(10);
        org.junit.Assert.assertNotNull(iface7);
    }

    @Test
    public void test0445() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0445");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0446() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0446");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0447() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0447");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setWatch("");
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
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(monitor26);
        org.junit.Assert.assertNotNull(iface37);
    }

    @Test
    public void test0448() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0448");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0449() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0449");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setInterval((int) (short) 10);
        monitor0.setWatch("");
        monitor0.setInterval(0);
    }

    @Test
    public void test0450() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0450");
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
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0451() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0451");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setInterval((int) (byte) -1);
        monitor0.setInterval((int) (short) -1);
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0452() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0452");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setInterval(0);
        monitor0.setWatch("");
        monitor0.setComponent("");
        monitor0.setInterval((int) ' ');
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0453() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0453");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0454() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0454");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setInterval((int) (short) 1);
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0455() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0455");
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
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0456() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0456");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
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
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: component: hi! not found");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor9);
        org.junit.Assert.assertNotNull(iface22);
    }

    @Test
    public void test0457() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0457");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setStream("hi!");
        monitor0.setComponent("hi!");
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
    public void test0458() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0458");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0459() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0459");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setComponent("");
        monitor0.setTopology("");
        org.apache.storm.generated.Nimbus.Iface iface19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
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
    public void test0460() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0460");
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
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface11);
    }

    @Test
    public void test0461() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0461");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setInterval((int) ' ');
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("");
        monitor0.setComponent("");
        monitor0.setInterval((int) (byte) 0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0462() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0462");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0463() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0463");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setInterval((-1));
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
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface30);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor13);
        org.junit.Assert.assertNotNull(iface30);
    }

    @Test
    public void test0464() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0464");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setWatch("");
    }

    @Test
    public void test0465() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0465");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("");
        monitor0.setWatch("");
        monitor0.setInterval((int) 'a');
        monitor0.setWatch("");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0466() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0466");
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
        org.apache.storm.generated.Nimbus.Iface iface19 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface19);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface19);
    }

    @Test
    public void test0467() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0467");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0468() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0468");
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
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0469() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0469");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setWatch("");
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0470() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0470");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setTopology("hi!");
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0471() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0471");
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
        monitor0.setComponent("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0472() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0472");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (byte) 10);
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0473() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0473");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("hi!");
        monitor0.setWatch("hi!");
        org.apache.storm.generated.Nimbus.Iface iface9 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface9);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: watch item must either be transferred or emitted");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface9);
    }

    @Test
    public void test0474() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0474");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0475() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0475");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setTopology("");
        java.lang.Class<?> wildcardClass9 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass9);
    }

    @Test
    public void test0476() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0476");
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
        monitor0.setInterval((int) 'a');
        monitor0.setWatch("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0477() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0477");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) -1);
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor11 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor11.setInterval((int) (short) -1);
        monitor11.setInterval((int) 'a');
        monitor11.setStream("");
        monitor11.setWatch("");
        monitor11.setInterval((int) (byte) 1);
        monitor11.setComponent("");
        org.apache.storm.generated.Nimbus.Iface iface24 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        monitor11.metrics(iface24);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface24);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: poll interval must be positive");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(monitor11);
        org.junit.Assert.assertNotNull(iface24);
    }

    @Test
    public void test0478() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0478");
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
        java.lang.Class<?> wildcardClass19 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass19);
    }

    @Test
    public void test0479() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0479");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("");
        monitor0.setTopology("");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
        monitor0.setWatch("");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0480() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0480");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        monitor0.setTopology("");
        monitor0.setTopology("hi!");
        monitor0.setInterval(100);
        monitor0.setTopology("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0481() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0481");
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
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0482() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0482");
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
    public void test0483() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0483");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setStream("");
        org.apache.storm.generated.Nimbus.Iface iface3 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusOtherComponents();
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface3);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(iface3);
    }

    @Test
    public void test0484() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0484");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setInterval((-1));
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0485() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0485");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0486() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0486");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setStream("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0487() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0487");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setTopology("hi!");
        monitor0.setInterval((int) (short) 1);
        monitor0.setStream("hi!");
        monitor0.setWatch("hi!");
        monitor0.setInterval((int) (short) 0);
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0488() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0488");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        monitor0.setInterval((int) (byte) 0);
        monitor0.setWatch("");
        java.lang.Class<?> wildcardClass17 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass17);
    }

    @Test
    public void test0489() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0489");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setComponent("");
        monitor0.setTopology("hi!");
        monitor0.setStream("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0490() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0490");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        monitor0.setTopology("");
        org.apache.storm.utils.Monitor monitor7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor7.setTopology("");
        monitor7.setComponent("hi!");
        monitor7.setStream("");
        monitor7.setComponent("");
        monitor7.setTopology("");
        monitor7.setStream("");
        monitor7.setWatch("hi!");
        monitor7.setTopology("hi!");
        org.apache.storm.generated.Nimbus.Iface iface24 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor7.metrics(iface24);
        org.apache.storm.utils.Monitor monitor26 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor26.setInterval((int) (short) -1);
        monitor26.setInterval((int) 'a');
        monitor26.setInterval((int) ' ');
        monitor26.setComponent("");
        monitor26.setTopology("hi!");
        monitor26.setInterval(10);
        org.apache.storm.utils.Monitor monitor39 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor39.setInterval((int) (short) -1);
        monitor39.setInterval((int) 'a');
        monitor39.setInterval((int) ' ');
        monitor39.setComponent("");
        monitor39.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface50 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbus();
        monitor39.metrics(iface50);
        monitor26.metrics(iface50);
        monitor7.metrics(iface50);
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface50);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: topology name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(monitor7);
        org.junit.Assert.assertNotNull(iface24);
        org.junit.Assert.assertNotNull(monitor26);
        org.junit.Assert.assertNotNull(monitor39);
        org.junit.Assert.assertNotNull(iface50);
    }

    @Test
    public void test0491() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0491");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setStream("hi!");
        monitor0.setTopology("hi!");
        monitor0.setStream("hi!");
        org.apache.storm.generated.Nimbus.Iface iface7 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMockNimbusIncompleteMetrics();
        monitor0.metrics(iface7);
        monitor0.setWatch("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        org.apache.storm.generated.Nimbus.Iface iface15 = null;
        // The following exception was thrown during execution in test generation
        try {
            monitor0.metrics(iface15);
            org.junit.Assert.fail("Expected exception of type java.lang.IllegalArgumentException; message: stream name must be something");
        } catch (java.lang.IllegalArgumentException e) {
            // Expected exception.
        }
        org.junit.Assert.assertNotNull(iface7);
    }

    @Test
    public void test0492() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0492");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval(100);
        monitor0.setInterval((-1));
        monitor0.setWatch("");
        monitor0.setWatch("hi!");
        monitor0.setTopology("hi!");
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
    public void test0493() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0493");
        org.apache.storm.utils.Monitor monitor0 = new org.apache.storm.utils.Monitor();
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setWatch("hi!");
    }

    @Test
    public void test0494() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0494");
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
        java.lang.Class<?> wildcardClass21 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass21);
    }

    @Test
    public void test0495() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0495");
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
        monitor0.setWatch("");
        org.junit.Assert.assertNotNull(monitor0);
    }

    @Test
    public void test0496() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0496");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setTopology("");
        monitor0.setInterval((int) '4');
        monitor0.setTopology("hi!");
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }

    @Test
    public void test0497() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0497");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setInterval((int) (short) -1);
        monitor0.setInterval((int) 'a');
        monitor0.setStream("");
        monitor0.setWatch("");
        monitor0.setTopology("hi!");
        java.lang.Class<?> wildcardClass11 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass11);
    }

    @Test
    public void test0498() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0498");
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
        org.junit.Assert.assertNotNull(iface11);
        org.junit.Assert.assertNotNull(monitor17);
        org.junit.Assert.assertNotNull(iface28);
    }

    @Test
    public void test0499() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0499");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setTopology("");
        monitor0.setInterval((int) (short) 10);
        monitor0.setInterval((int) (byte) -1);
        monitor0.setStream("");
        java.lang.Class<?> wildcardClass15 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass15);
    }

    @Test
    public void test0500() throws Throwable {
        if (debug)
            System.out.format("%n%s%n", "RegressionTest0.test0500");
        org.apache.storm.utils.Monitor monitor0 = org.apache.storm.utils.staticmocking.RandoopMonitorHelper.createMonitor();
        monitor0.setTopology("");
        monitor0.setComponent("hi!");
        monitor0.setStream("");
        monitor0.setComponent("hi!");
        monitor0.setStream("hi!");
        monitor0.setComponent("");
        java.lang.Class<?> wildcardClass13 = monitor0.getClass();
        org.junit.Assert.assertNotNull(monitor0);
        org.junit.Assert.assertNotNull(wildcardClass13);
    }
}

