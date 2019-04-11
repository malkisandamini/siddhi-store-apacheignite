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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ApacheIgniteTestUtils {

    public static final String URL = "jdbc:ignite:thin://127.0.0.1/";
    public static final String PASSWORD = "ignite";
    public static final String USERNAME = "ignite";
    public static final String TABLE_NAME = "stocktable";

    private static final Log log = LogFactory.getLog(ApacheIgniteTestUtils.class);

    private ApacheIgniteTestUtils() {

    }

    public static void dropTable(String tableName) throws SQLException {

        try {
            Connection con = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
            PreparedStatement st = con.prepareStatement("drop table " + tableName);
            st.execute();
        } catch (SQLException e) {
            log.debug("clearing table failed due to " + e.getMessage());
        }
    }

}
