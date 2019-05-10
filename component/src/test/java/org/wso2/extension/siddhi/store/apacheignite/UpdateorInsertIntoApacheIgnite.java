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
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.exception.DuplicateDefinitionException;

import java.sql.SQLException;

import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.URL;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.USERNAME;

public class UpdateorInsertIntoApacheIgnite {

    private static final Log log = LogFactory.getLog(UpdateorInsertIntoApacheIgnite.class);

    @BeforeClass
    public static void startTest() {

        log.info("== Apache Ignite Table UPDATE/INSERT tests started ==");
    }

    @AfterClass
    public static void shutdown() {

        log.info("== Apache Ignite Table UPDATE/INSERT tests completed ==");
    }

    @BeforeMethod
    public void init() {

        try {
            ApacheIgniteTestUtils.dropTable(TABLE_NAME);

        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @Test(description = "Testing updating or inserting with stream variable")
    public void updateOrInsertIntoTableTest() throws InterruptedException, SQLException {

        log.info("UpdateOrInsertIntoTableTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "on StockTable.symbol==symbol;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WS", 325.6f, 100L});
        stockStream.send(new Object[]{"IB", 75.6f, 100L});
        updateStockStream.send(new Object[]{"GOOG", 12.6F, 100L});
        updateStockStream.send(new Object[]{"GOOG", 1278.6F, 200L});
        updateStockStream.send(new Object[]{"IB", 27.6F, 101L});
        Thread.sleep(500);
        int rowsInTable = ApacheIgniteTestUtils.getRowsInTable(TABLE_NAME);
        Assert.assertEquals(rowsInTable, 3, "Insertion/Updating failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing updating or inserting with constants")
    public void updateOrInsertIntoTableTest2() throws InterruptedException, SQLException {

        log.info("UpdateOrInsertIntoTableTest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "on StockTable.symbol=='WS';";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WS", 325.6f, 100L});
        stockStream.send(new Object[]{"IB", 75.6f, 100L});
        updateStockStream.send(new Object[]{"GOOG", 12.6F, 100L});
        updateStockStream.send(new Object[]{"GOOG", 1278.6F, 200L});
        updateStockStream.send(new Object[]{"IB", 27.6F, 101L});
        Thread.sleep(500);
        int rowsInTable = ApacheIgniteTestUtils.getRowsInTable(TABLE_NAME);
        Assert.assertEquals(rowsInTable, 3, "Insertion/Updating failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing updating or inserting with non primary key attributes ")
    public void updateOrInsertIntoTableTest3() throws InterruptedException, SQLException {

        log.info("UpdateOrInsertIntoTableTest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "on StockTable.volume==volume;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WS", 325.6f, 100L});
        stockStream.send(new Object[]{"IB", 75.6f, 100L});
        updateStockStream.send(new Object[]{"GOOG", 12.6F, 100L});
        updateStockStream.send(new Object[]{"GOOG", 1278.6F, 200L});
        updateStockStream.send(new Object[]{"IB", 27.6F, 101L});
        Thread.sleep(500);
        int rowsInTable = ApacheIgniteTestUtils.getRowsInTable(TABLE_NAME);
        Assert.assertEquals(rowsInTable, 3, "Insertion/Updating failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing updating or inserting with duplicate stream definitions ",
            expectedExceptions = DuplicateDefinitionException.class)
    public void updateOrInsertIntoTableTest4() throws InterruptedException, SQLException {

        log.info("UpdateOrInsertIntoTableTest4");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "on StockTable.symbol==symbol;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WS", 325.6f, 100L});
        stockStream.send(new Object[]{"IB", 75.6f, 100L});
        updateStockStream.send(new Object[]{"GOOG", 12.6F});
        updateStockStream.send(new Object[]{"GOOG", 1278.6F});
        siddhiAppRuntime.shutdown();
    }

}

