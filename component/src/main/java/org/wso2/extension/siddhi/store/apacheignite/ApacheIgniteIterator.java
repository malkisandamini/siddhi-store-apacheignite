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

import org.wso2.siddhi.core.table.record.RecordIterator;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * class representing record iterator
 */
public class ApacheIgniteIterator implements RecordIterator<Object[]> {

//    private Connection connection;
//    private PreparedStatement statement;
    private ResultSet resultSet;
    private String tableName;

    private List<Attribute> attributes;
    private boolean preFetched;
    private Object[] nextValue;

    public ApacheIgniteIterator(Connection con, PreparedStatement statement, ResultSet rs, String tableName,
                                List<Attribute> attributes) {

//        this.connection = con;
//        this.statement = statement;
        this.resultSet = rs;
        this.tableName = tableName;
        this.attributes = attributes;
    }

    @Override
    public boolean hasNext() {

        if (!this.preFetched) {
            this.nextValue = this.next();
            this.preFetched = true;
        }
        return nextValue != null;
    }

    @Override
    public Object[] next() {

        if (this.preFetched) {
            this.preFetched = false;
            Object[] result = this.nextValue;
            this.nextValue = null;
            return result;
        }
        try {
            if (this.resultSet.next()) {
                return this.extractRecord(this.resultSet);
            } else {

                return new Object[0];
            }
        } catch (SQLException e) {

            throw new ApacheIgniteTableException("Error retrieving records from table '" + this.tableName + "': "
                    + e.getMessage(), e);
        }
    }

    public Object[] extractRecord(ResultSet rs) throws SQLException {

        List<Object> result = new ArrayList<>();
        for (Attribute attribute : attributes) {
            switch (attribute.getType()) {
                case BOOL:
                    result.add(rs.getBoolean(attribute.getName()));
                    break;
                case DOUBLE:
                    result.add(rs.getDouble(attribute.getName()));
                    break;
                case FLOAT:
                    result.add(rs.getFloat(attribute.getName()));
                    break;
                case INT:
                    result.add(rs.getInt(attribute.getName()));
                    break;
                case LONG:
                    result.add(rs.getLong(attribute.getName()));
                    break;
                case OBJECT:
                    result.add(rs.getObject(attribute.getName()));
                    break;
                case STRING:
                    result.add(rs.getString(attribute.getName()));
                    break;
            }
        }
        return result.toArray();
    }

    @Override
    public void close() throws IOException {

    }

}
