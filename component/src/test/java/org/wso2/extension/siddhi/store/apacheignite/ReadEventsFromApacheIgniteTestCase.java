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

import java.sql.SQLException;

import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.URL;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.USERNAME;

public class ReadEventsFromApacheIgniteTestCase {

    private static final Log log = LogFactory.getLog(UpdateFromApacheIgniteTestCase.class);

    private int removeEventCount;
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

    @Test(description = "Testing reading  ")
    public void readIntoTableTest() throws InterruptedException {

        log.info("ReadIntoTableTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string);\n" +
                "define stream OutputStream (checkName string, checkCategory float, checkVolume long);" +
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
                //"from FooStream#window.length(1) join StockTable on StockTable.volume==FooStream.name \n" +
                //"from FooStream#window.length(1) join StockTable on StockTable.price+40>FooStream.name \n" +
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
        stockStream.send(new Object[]{"WS", 325.6f, 100L});
        stockStream.send(new Object[]{"IB", 75.6f, 100L});
        stockStream.send(new Object[]{"GOOG", 12.6F, 100L});

        fooStream.send(new Object[]{"WS"});
        fooStream.send(new Object[]{"CSC"});
        fooStream.send(new Object[]{"IB"});

        Thread.sleep(500);
        //Assert.assertEquals(eventArrived, true, "Event arrived");
        Thread.sleep(500);
        int pointsInTable = 2;
        // Assert.assertEquals(pointsInTable, 2, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

}
