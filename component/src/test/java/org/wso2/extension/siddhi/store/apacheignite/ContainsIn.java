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
package org.wso2.extension.siddhi.store.apacheignite;

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
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.URL;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.USERNAME;

public class ContainsIn {

    private static final Log log = LogFactory.getLog(UpdateFromApacheIgniteTestCase.class);
    private AtomicInteger eventCount = new AtomicInteger(0);
    private int inEventCount;
    private int waitTime = 500;
    private int timeout = 30000;
    private boolean eventArrived;

    @BeforeClass
    public static void startTest() {

        log.info("test started");
    }

    @AfterClass
    public static void shutdown() {

        log.info("test completed");
    }

    @BeforeMethod
    public void init() {

        try {
            ApacheIgniteTestUtils.dropTable(TABLE_NAME);

        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @Test(description = "Testing contains in  ")
    public void containsInTableTest() throws InterruptedException {

        log.info("containsInTableTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (name string,value long);\n" +
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
                "from FooStream \n" +
                "[(StockTable.volume ==value ) in StockTable]\n" +
                "insert into OutputStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        // InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long l, Event[] events, Event[] events1) {

                EventPrinter.print(l, events, events1);
                if (events != null) {
                    eventArrived = true;
                    inEventCount++;
                    for (Event event : events) {
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(new Object[]{"WS", 100}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"IB", 10}, event.getData());
                                break;
                        }
                    }
                } else {
                    eventArrived = false;
                }
            }
        });
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WS", 325.6f, 100L});
        stockStream.send(new Object[]{"IB", 75.6f, 100L});
        stockStream.send(new Object[]{"GOOG", 12.6F, 100L});

        Thread.sleep(500);
        int pointsInTable = 2;

        fooStream.send(new Object[]{"WS", 100});
        fooStream.send(new Object[]{"IB", 10});
        fooStream.send(new Object[]{"WSO22", 100});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertEquals(eventArrived, true, "success");
        Assert.assertEquals(pointsInTable, 2, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

}
