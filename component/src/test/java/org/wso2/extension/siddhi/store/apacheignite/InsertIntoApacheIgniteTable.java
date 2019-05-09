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

import java.sql.SQLException;

import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.URL;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteTestUtils.USERNAME;

public class InsertIntoApacheIgniteTable {

    private static final Log log = LogFactory.getLog(InsertIntoApacheIgniteTable.class);

    @BeforeClass
    public static void startTest() {

        log.info("== Apache Ignite Table INSERT tests started ==");
    }

    @AfterClass
    public static void shutdown() {

        log.info("==  Apache Ignite Table INSERT tests completed==");
    }

    @BeforeMethod
    public void init() {

        try {
            ApacheIgniteTestUtils.dropTable(TABLE_NAME);
        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @Test(description = "Testing insertion with single primary key ")
    public void insertIntoTableWithSinglePrimaryKeyTest() throws InterruptedException, SQLException {

        log.info("insertIntoTableWithSinglePrimaryKeyTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 325.6f, 100L});
        stockStream.send(new Object[]{"IB", 75.6f, 100L});
        Thread.sleep(500);
        int rowsInTable = ApacheIgniteTestUtils.getRowsInTable(TABLE_NAME);
        Assert.assertEquals(rowsInTable, 2, "Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing insertion with duplicate primary key ")
    public void insertIntoTableWithDuplicatePrimaryKeyTest() throws InterruptedException, SQLException {

        log.info("insertIntoTableWithDuplicatePrimaryKeyTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WS", 325.6f, 100L});
        stockStream.send(new Object[]{"IB", 75.6f, 100L});
        stockStream.send(new Object[]{"WS", 25.6f, 100L});
        Thread.sleep(500);
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing insertion with two primary keys ")
    public void insertIntoTableWithTwoPrimaryKeysTest() throws InterruptedException, SQLException {

        log.info("insertIntoTableWithTwoPrimaryKeysTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\",\"price\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WS", 325.6f, 100L});
        stockStream.send(new Object[]{"IB", 75.6f, 100L});
        Thread.sleep(500);
        int rowsInTable = ApacheIgniteTestUtils.getRowsInTable(TABLE_NAME);
        Assert.assertEquals(rowsInTable, 2, "Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing insertion with specific attributes ")
    public void insertIntoTableSelectingSpecificAttributes() throws InterruptedException, SQLException {

        log.info("insertIntoTableSelectingSpecificAttributesTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float); ";
        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WS", 325.6f});
        stockStream.send(new Object[]{"IB", 75.6f});
        Thread.sleep(500);
        int rowsInTable = ApacheIgniteTestUtils.getRowsInTable(TABLE_NAME);
        Assert.assertEquals(rowsInTable, 2, "Insertion failed");
        siddhiAppRuntime.shutdown();
    }

}
