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
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.StoreQueryCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.sql.SQLException;

import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteTestUtils.URL;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteTestUtils.USERNAME;

public class StoreQueryTestCase {

    private static final Log log = LogFactory.getLog(StoreQueryTestCase.class);

    @BeforeClass
    public static void startTest() {

        log.info("== Apache Ignite Table Store Query tests started ==");
    }

    @AfterClass
    public static void shutdown() {

        log.info("== Apache Ignite Table Store Query tests completed ==");
    }

    @BeforeMethod
    public void init() {

        try {
            ApacheIgniteTestUtils.dropTable(ApacheIgniteTestUtils.TABLE_NAME);

        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @Test(description = "testing with different ways of on condition")
    public void storeQueryTest1() throws InterruptedException {

        log.info("Test1 table");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO22", 57.6f, 100L});
        Thread.sleep(500);
        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "select*");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(3, events.length);
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > 75 ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > volume*3/4  ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "testing by selecting specific attributes")
    public void storeQueryTest2() throws InterruptedException {

        log.info("Test2 table");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO22", 57.6f, 100L});
        Thread.sleep(500);
        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > 75 " +
                "select symbol, volume ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        AssertJUnit.assertEquals(2, events[0].getData().length);
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "select symbol, price ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(3, events.length);
        AssertJUnit.assertEquals(2, events[0].getData().length);
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > 5 " +
                "select symbol " +
                "group by symbol " +
                "having symbol == 'WSO2' ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void storeQueryTest3() throws InterruptedException {

        log.info("Test3 table");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        Thread.sleep(500);
        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > 5 " +
                "select symbol, volume " +
                "group by symbol " +
                "having symbol == 'WSO2' ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        AssertJUnit.assertEquals(100L, events[0].getData(1));
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > 5 " +
                "select symbol, volume " +
                "group by symbol  ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > 5 " +
                "select symbol, volume " +
                "group by symbol,price  ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = StoreQueryCreationException.class)
    public void test4() throws InterruptedException {

        log.info("Test4 table");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        try {
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            siddhiAppRuntime.start();
            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO22", 57.6f, 100L});
            Thread.sleep(500);
            Event[] events = siddhiAppRuntime.query("" +
                    "from StockTable " +
                    "on price > 5 " +
                    "select symbol1, volume " +
                    "group by symbol " +
                    "having totalVolume >150 ");
            EventPrinter.print(events);
            AssertJUnit.assertEquals(1, events.length);
            AssertJUnit.assertEquals(400L, events[0].getData(1));
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(description = "testing with constant value for on condition")
    public void test5() throws InterruptedException {

        log.info("Test5 table");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long);" +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        Thread.sleep(500);
        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on symbol == 'IBM' " +
                "select symbol, volume ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        AssertJUnit.assertEquals("IBM", events[0].getData()[0]);
    }

    @Test(description = "testing order by and limit")
    public void test6() throws InterruptedException {

        log.info("Test6 table");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long);" +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        Thread.sleep(500);
        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on volume > 10 " +
                "select symbol, price, volume " +
                "order by price DESC " +
                "limit 2 ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);
        AssertJUnit.assertEquals(75.6F, events[0].getData()[1]);
        AssertJUnit.assertEquals(55.6f, events[1].getData()[1]);
    }

    @Test(description = "Testing group by and having")
    public void havingtTest7() throws InterruptedException {

        log.info(" havingtTest ");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long);" +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        Thread.sleep(500);
        String storeQuery = "" +
                "from StockTable " +
                "on volume > 10 " +
                "select symbol, volume  " +
                "group by symbol " +
                "having symbol == 'WSO2'";
        Event[] events = siddhiAppRuntime.query(storeQuery);
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        AssertJUnit.assertEquals(100L, events[0].getData()[1]);
    }

    @Test(description = "Testing group by")
    public void groupByTest() throws InterruptedException {

        log.info("groupByTest ");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long);" +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        Thread.sleep(500);
        String storeQuery = "" +
                "from StockTable " +
                "on price > 56 " +
                "select symbol, price, volume " +
                "group by symbol, price ";
        Event[] events = siddhiAppRuntime.query(storeQuery);
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        AssertJUnit.assertEquals(100L, events[0].getData()[2]);

    }

    @Test
    public void storeQueryTest() throws InterruptedException {

        log.info("storeQueryTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name='query2') " +
                "from StockStream\n" +
                "select symbol,price,volume\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"CSC", 57.6f, 100L});
        stockStream.send(new Object[]{"WSO22", 59.6f, 100L});
        Thread.sleep(500);
        Event[] events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > 5 AND symbol==\"WSO2\" " +
                "select * " +
                "order by price DESC");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on volume > 10 " +
                "select symbol,volume  " +
                "limit 2 ");
        EventPrinter.print(events);
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on price > 5 " +
                "select* " +
                "group by symbol  ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(4, events.length);
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on volume > 10 " +
                "select symbol, price " +
                "offset 1 ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(3, events.length);
        events = siddhiAppRuntime.query("" +
                "from StockTable " +
                "on volume > 10 " +
                "select symbol, volume  " +
                "group by symbol " +
                "having symbol == 'WSO2'");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void test9() throws InterruptedException {

        log.info("Test9");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 200L});
        stockStream.send(new Object[]{"GOOGLE", 57.6f, 300L});
        Thread.sleep(500);

        Event[] initialEvents = siddhiAppRuntime.query("from StockTable select *;");
        Assert.assertEquals(initialEvents.length, 3);

        siddhiAppRuntime.query("delete StockTable on StockTable.volume == 100L;");
        Thread.sleep(500);

        Event[] allEvents = siddhiAppRuntime.query("from StockTable select *;");
        Assert.assertEquals(allEvents.length, 2);

        Event[] events = siddhiAppRuntime.query("from StockTable on volume == 100L select *");
        Assert.assertNull(events);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing store query with limit and offset")
    public void limitAndOffsettest() throws InterruptedException {

        log.info("Testing store query with limit and offset");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream StockStream (id int, symbol string, volume int); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (id int, symbol string, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{1, "WSO2", 100});
        stockStream.send(new Object[]{2, "IBM", 200});
        stockStream.send(new Object[]{3, "GOOGLE", 300});
        Thread.sleep(500);

        Event[] allEvents = siddhiAppRuntime.query("from StockTable select * LIMIT 2;");
        Assert.assertEquals(allEvents.length, 2);
        Assert.assertEquals(allEvents[0].getData()[0], 3);
        Assert.assertEquals(allEvents[1].getData()[0], 2);

        allEvents = siddhiAppRuntime.query("from StockTable select * LIMIT 1 OFFSET 0;");
        Assert.assertEquals(allEvents.length, 1);
        Assert.assertEquals(allEvents[0].getData()[0], 3);

        allEvents = siddhiAppRuntime.query("from StockTable select * LIMIT 1 OFFSET 1;");
        Assert.assertEquals(allEvents.length, 1);
        Assert.assertEquals(allEvents[0].getData()[0], 2);
    }
}


