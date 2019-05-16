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
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.sql.SQLException;

import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteTestUtils.URL;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteTestUtils.USERNAME;

public class DefineTableTestCase {

    private static final Log log = LogFactory.getLog(DefineTableTestCase.class);

    @BeforeClass
    public static void startTest() {

        log.info("== Apache Ignite define Table tests started ==");
    }

    @AfterClass
    public static void shutdown() {

        log.info("==  Apache Ignite define Table tests completed==");
    }

    @BeforeMethod
    public void init() {

        try {
            ApacheIgniteTestUtils.dropTable(ApacheIgniteTestUtils.TABLE_NAME);

        } catch (SQLException e) {
            log.info("Test case ignored due to " + e.getMessage());
        }
    }

    @Test(description = "Testing table creation with single primary key ")
    public void createTableWithSinglePrimaryKeyTest() throws InterruptedException, SQLException {

        log.info("createTableWithSinglePrimaryKeyTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," + "auth.enabled=\"false\"," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\",backups=" + "\"2\")\n" +
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
        Thread.sleep(500);
        int rowsInTable = ApacheIgniteTestUtils.getRowsInTable(ApacheIgniteTestUtils.TABLE_NAME);
        Assert.assertEquals(rowsInTable, 2, "Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing table creation with no primary key ",
            expectedExceptions = SiddhiAppCreationException.class)
    public void createTableWithNoPrimaryKeyTest() {

        log.info("createTableWithNoPrimaryKeyTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "define table StockTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing table creation without defining value for url ",
            expectedExceptions = SiddhiAppCreationException.class)
    public void createTableWithTest() {

        log.info("createTableWithoutUrlTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing table creation without url field ",
            expectedExceptions = SiddhiAppCreationException.class)
    public void createTableWithoutUrlFieldTest()  {

        log.info("createTableWithoutUrlField");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", " +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "insert into StockTable ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing table creation with invalid url ")
    public void createTableWithInvalidUrlTest() {

        log.info("createTableWithInvalidUrlTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + "jdbc:ignite:thi://127.0.1.1/" + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing table creation without defining value for username field ",
            expectedExceptions = SiddhiAppCreationException.class)
    public void createTableWithoutDefineUsernameTest() {

        log.info("createTableWithoutDefineUsernameTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," + "auth.enabled=\"true\"," +
                "username=\"" + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing table creation with incorrect username field ")
    public void createTableWithIncorrectUsernameTest() {

        log.info("createTableWithIncorrectUsernameTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," + "auth.enabled=\"true\"," +
                "username=\"" + "ignit" + "\", password=\"" + PASSWORD
                + "\")\n" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(description = "Testing table creation with table.name ")
    public void createTableTest() throws InterruptedException, SQLException {

        ApacheIgniteTestUtils.dropTable("FooTable");
        log.info("createTableTest");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\",table.name=\"FooTable\")\n" +
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
        Thread.sleep(100);
        int rowsInTable = ApacheIgniteTestUtils.getRowsInTable("FooTable");
        Assert.assertEquals(rowsInTable, 2, "Insertion failed");
        siddhiAppRuntime.shutdown();

    }

    @Test(description = "Testing table creation with different parameters for table creation")
    public void createTableWithParametersTest1() throws InterruptedException, SQLException {

        log.info("createTableWithParametersTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\",template=\"replicated\",backups= \"2\",atomicity= \"transactional\"," +
                "affinityKey= \"symbol\" ,cacheName= \"stockTable\")\n" +
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
        Thread.sleep(500);
        int rowsInTable = ApacheIgniteTestUtils.getRowsInTable(ApacheIgniteTestUtils.TABLE_NAME);
        Assert.assertEquals(rowsInTable, 2, "Insertion failed");
        siddhiAppRuntime.shutdown();

    }

    @Test(description = "Testing table creation with different parameters with connection url")
    public void createTableWithParametersTest2() throws InterruptedException, SQLException {

        log.info("createTableWithParametersTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"apacheignite\", url = \"" + URL + "\" ," +
                "username=\"" + USERNAME + "\", password=\"" + PASSWORD
                + "\",socketSendBuffer=\"10\",socketRecieveBuffer= \"10\",replicatedOnly= \"true\"," +
                "affinityKey= \"symbol\" ,cacheName= \"stockTable\")\n" +
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
        Thread.sleep(500);
        int rowsInTable = ApacheIgniteTestUtils.getRowsInTable(ApacheIgniteTestUtils.TABLE_NAME);
        Assert.assertEquals(rowsInTable, 2, "Insertion failed");
        siddhiAppRuntime.shutdown();

    }
}
