/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.extension.siddhi.store.ignite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

import java.sql.SQLException;

import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteTestUtils.URL;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteTestUtils.USERNAME;

public class ReadEventsFromApacheIgniteTestCase {

    private static final Log log = LogFactory.getLog(ReadEventsFromApacheIgniteTestCase.class);

    private int removeEventCount;
    private boolean eventArrived;

    @BeforeClass
    public static void startTest() {

        log.info("== Apache Ignite Table READ tests started ==");
    }

    @AfterClass
    public static void shutdown() {

        log.info("== Apache Ignite Table READ tests completed ==");
    }

    @BeforeMethod
    public void init() {

        try {
            removeEventCount = 0;
            ApacheIgniteTestUtils.dropTable(ApacheIgniteTestUtils.TABLE_NAME);

        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @Test(description = "Testing reading using primary key")
    public void readFromTableTest() throws InterruptedException {

        log.info("ReadFromTableTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string);\n" +
                "define stream OutputStream (checkName string, checkVolume long,checkCategory float);" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n " +
                "from FooStream#window.length(1) join StockTable on StockTable.symbol==FooStream.name \n" +
                "select StockTable.symbol as checkName, " +
                "StockTable.volume as checkVolume," +
                "StockTable.price as checkCategory\n " +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 325.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"GOOG", 12.6F, 100L});
        fooStream.send(new Object[]{"WSO2"});
        fooStream.send(new Object[]{"CSC"});
        fooStream.send(new Object[]{"IBM"});
        Thread.sleep(500);
        Assert.assertTrue(eventArrived, "Event arrived");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events ");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing reading with constants  ")
    public void readFromTableTest2() throws InterruptedException {

        log.info("ReadFromTableTest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (vol long);\n" +
                "define stream OutputStream (checkName string, checkVolume long,checkCategory float);" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n " +
                "from FooStream#window.length(1) join StockTable on StockTable.volume+50==FooStream.vol \n" +
                "select StockTable.symbol as checkName, " +
                "StockTable.volume as checkVolume," +
                "StockTable.price as checkCategory\n " +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WS", 325.6f, 100L});
        stockStream.send(new Object[]{"IB", 75.6f, 100L});
        stockStream.send(new Object[]{"GOOG", 12.6F, 150L});
        fooStream.send(new Object[]{150});
        fooStream.send(new Object[]{200});
        Thread.sleep(500);
        Assert.assertTrue(eventArrived, "Event arrived");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing reading by selecting specific fields ")
    public void readFromTableTest3() throws InterruptedException {

        log.info("ReadFromTableTest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name long);\n" +
                "define stream OutputStream (checkName string,checkCategory float);" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n " +
                "from FooStream#window.length(1) join StockTable on StockTable.volume+50==FooStream.name \n" +
                "select StockTable.symbol as checkName, " +
                "StockTable.price as checkCategory\n " +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WS", 325.6f, 100L});
        stockStream.send(new Object[]{"IB", 75.6f, 100L});
        stockStream.send(new Object[]{"GOOG", 12.6F, 150L});
        fooStream.send(new Object[]{150});
        fooStream.send(new Object[]{200});
        Thread.sleep(500);
        Assert.assertTrue(eventArrived, "Event arrived");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiParserException.class, description = "Read unsuccessfully from non existing table")
    public void readFromNonExistingTableTest() throws InterruptedException {

        log.info("readFromNonExistingTableTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (symbol string);";
        String query = "" +
                "@info(name = 'query2')\n " +
                "from FooStream#window.length(2) join StockTable on StockTable.symbol==FooStream.name \n" +
                "select StockTable.symbol as checkName, StockTable.price as checkCategory, " +
                "StockTable.volume as checkVolume\n" +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        fooStream.send(new Object[]{"WSO2"});
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiParserException.class, description = "Read unsuccessfully without defining stream")
    public void readEventWithoutDefineStreamTest() throws InterruptedException {

        log.info("readEventWithoutDefineStreamTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long);\n" +
                "define stream OutputStream (checkName string, checkCategory float, checkVolume long);" +
                "\n" +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n " +
                "from FooStream#window.length(2) join StockTable on StockTable.symbol==FooStream.name \n" +
                "select StockTable.symbol as checkName, StockTable.price as checkCategory, " +
                "StockTable.volume as checkVolume\n" +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 58.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        stockStream.send(new Object[]{"IBM", 97.6f, 100L});
        fooStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);
        Assert.assertTrue(eventArrived, "Event arrived");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Read  multiple events from a apache ignite table successfully with window.time.")
    public void readEventsFromIgniteTableTest() throws InterruptedException {

        log.info("readEventsFromTableTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string);\n" +
                "define stream StockStream (symbol string, price float, volume long);\n" +
                "define stream OutputStream (checkName string, checkCategory float, checkVolume long);" +
                "\n" +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n " +
                "from FooStream#window.length(1) join StockTable on StockTable.symbol==FooStream.name \n" +
                "select StockTable.symbol as checkName, StockTable.price as checkCategory, " +
                "StockTable.volume as checkVolume\n" +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 58.6f, 110L});
        stockStream.send(new Object[]{"IBM", 57.6f, 100L});
        stockStream.send(new Object[]{"CSC", 97.6f, 50L});
        stockStream.send(new Object[]{"MIT", 97.6f, 10L});
        fooStream.send(new Object[]{"WSO2"});
        fooStream.send(new Object[]{"IBM"});
        fooStream.send(new Object[]{"MIT"});
        Thread.sleep(1000);
        Assert.assertTrue(eventArrived, "Event arrived");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Read  multiple events from a apache ignite table successfully with window.length.")
    public void readEventsFromApacheIgniteTableTest() throws InterruptedException {

        log.info("readEventsFromTableTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string);\n" +
                "define stream StockStream (symbol string, price float, volume long);\n" +
                "define stream OutputStream (checkName string, checkCategory float, checkVolume long);" +
                "\n" +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n " +
                "from FooStream#window.length(1) join StockTable on StockTable.symbol==FooStream.name \n" +
                "select StockTable.symbol as checkName, StockTable.price as checkCategory, " +
                "StockTable.volume as checkVolume\n" +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 58.6f, 110L});
        stockStream.send(new Object[]{"IBM", 57.6f, 100L});
        stockStream.send(new Object[]{"CSC", 97.6f, 50L});
        stockStream.send(new Object[]{"MIT", 97.6f, 10L});
        fooStream.send(new Object[]{"WSO2"});
        fooStream.send(new Object[]{"IBM"});
        fooStream.send(new Object[]{"MIT"});
        Thread.sleep(1000);
        Assert.assertTrue(eventArrived, "Event arrived");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing reading without any condition ")
    public void readFromTableWithoutConditionTest() throws InterruptedException {

        log.info("readFromTableWithoutConditionTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string);\n" +
                "define stream OutputStream (checkName string, checkVolume long,checkCategory float);" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n " +
                "from FooStream#window.length(1) join StockTable \n" +
                "select StockTable.symbol as checkName, " +
                "StockTable.volume as checkVolume," +
                "StockTable.price as checkCategory\n " +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 325.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"GOOG", 12.6F, 100L});
        fooStream.send(new Object[]{"WSO2"});
        Thread.sleep(500);
        Assert.assertTrue(eventArrived, "Event arrived");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events ");
        siddhiAppRuntime.shutdown();
    }
}
