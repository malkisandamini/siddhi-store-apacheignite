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
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.sql.SQLException;

import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.URL;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.USERNAME;

public class DeleteFromApacheIgniteTestCase {

    private static final Log log = LogFactory.getLog(DeleteFromApacheIgniteTestCase.class);

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

    @Test(description = "Testing insertion ")
    public void deleteFromTableTest() throws InterruptedException {

        log.info("insertIntoTableWithSingleTagKeyTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 325.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"CSC", 85.6f, 200L});
        deleteStockStream.send(new Object[]{"WSO2"});
        deleteStockStream.send(new Object[]{"IBM"});
        //deleteStockStream.send(new Object[]{75.6f});
        Thread.sleep(500);
//        int pointsInTable = 1;
//        Assert.assertEquals(pointsInTable, 1, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing insertion ")
    public void deleteFromTableTest2() throws InterruptedException {

        log.info("insertIntoTableTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == 'WSO2' ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 325.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"CSC", 85.6f, 200L});
        deleteStockStream.send(new Object[]{"WSO2"});
        deleteStockStream.send(new Object[]{"IBM"});
        //deleteStockStream.send(new Object[]{75.6f});
        Thread.sleep(500);
//        int pointsInTable = 1;
//        Assert.assertEquals(pointsInTable, 1, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

}
