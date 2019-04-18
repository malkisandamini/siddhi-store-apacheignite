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
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.sql.SQLException;

import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.URL;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.USERNAME;

public class StoreQueryTestCase {

    private static final Log log = LogFactory.getLog(StoreQueryTestCase.class);

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
    @Test
    public void storeQueryTest1() throws InterruptedException {
        log.info("storeQueryTest1");
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
//                "from StockTable " +
//                "on price > 5 AND symbol==\"WSO2\" " +
//                "select * " +
//                "order by price DESC");
//        EventPrinter.print(events);
        //AssertJUnit.assertEquals(3, events.length);
       // events = siddhiAppRuntime.query("" +
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
//        AssertJUnit.assertEquals(4, events.length);
//        events = siddhiAppRuntime.query("" +
//                "from StockTable " +
//                "on volume > 10 " +
//                "select symbol, price " +
//                "offset 1 ");
//        EventPrinter.print(events);
////        AssertJUnit.assertEquals(1, events.length);
//        events = siddhiAppRuntime.query("" +
//        "from StockTable " +
//                "on volume > 10 " +
//                "select symbol, volume as totalVolume " +
//                "group by symbol "+
//                "having symbol == 'WSO2'");
//        EventPrinter.print(events);
//        AssertJUnit.assertEquals(1, events.length);
        siddhiAppRuntime.shutdown();

    }

    @Test
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
                "from StockTable ");
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
}
