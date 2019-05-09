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
//import org.apache.ignite.IgniteJdbcThinDriver;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.table.record.AbstractQueryableRecordTable;
import org.wso2.siddhi.core.table.record.ExpressionBuilder;
import org.wso2.siddhi.core.table.record.RecordIterator;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.CompiledExpression;
import org.wso2.siddhi.core.util.collection.operator.CompiledSelection;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.annotation.Element;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.TableDefinition;
import org.wso2.siddhi.query.api.execution.query.selection.OrderByAttribute;
import org.wso2.siddhi.query.api.util.AnnotationHelper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.AFFINITY_KEY;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.ANNOTATION_ELEMENT_AUTH_ENABLED;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.ANNOTATION_ELEMENT_PASSWORD;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.ANNOTATION_ELEMENT_TABLE_NAME;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.ANNOTATION_ELEMENT_URL;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.ANNOTATION_ELEMENT_USERNAME;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.ATOMICITY;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.AUTO_CLOSE_SERVER_CURSER;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.BACKUPS;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.BOOLEAN;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.CACHE_NAME;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.CLOSE_PARENTHESIS;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.COLLOCATED;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.COLUMNS;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.DATA_REGION;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.DISTRIBUTED_JOINS;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.ENFORCE_JOIN_ORDER;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.EQUAL;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.FLOAT;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.INSERT_QUERY;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.LONG;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.OPEN_PARENTHESIS;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.REPLICATED_ONLY;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.SCHEMA;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.SEMI_COLON;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.SEPARATOR;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.SOCKET_RECEIVE_BUFFER;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.SOCKET_SEND_BUFFER;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.SQL_PRIMARY_KEY_DEF;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.STRING;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.TABLE_CREATE_QUERY;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.TABLE_NAME;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.TEMPLATE;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.VALUES;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.WHITESPACE;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_INDEX;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_PRIMARY_KEY;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_STORE;

/**
 * Class representing apacheIgnite store implementation.
 */
@Extension(
        name = "apacheignite",
        namespace = "store",
        description = "This extension connects to apache Ignite store." +
                "It also implements read-write operations on connected apache ignite data store.",
        parameters = {
               /* @Parameter(
                        name = " ",
                        description = " " ,
                        dynamic = false/true,
                        optional = true/false, defaultValue = " ",
                        type = {DataType.INT, DataType.BOOL, DataType.STRING, DataType.DOUBLE, }
                        ),*/
                @Parameter(
                        name = "auth.enabled",
                        description = "Describes whether authentication is enabled or not ",
                        optional = true,
                        defaultValue = "false",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "username",
                        description = "username for SQL connection.Mandatory parameter if the authentication" +
                                " is enabled on the server ",
                        optional = true,
                        defaultValue = "ignite ",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "password",
                        description = "password for SQL connection.Mandatory parameter if the authentication " +
                                "is enabled on the server. ",
                        optional = true, defaultValue = "ignite",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "schema",
                        description = "Schema name to access ",
                        optional = true,
                        defaultValue = "Public ",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "template",
                        description = " name of a cache template registered in Ignite to use as a configuration " +
                                "for the distributed cache ",
                        optional = true,
                        defaultValue = "partitioned ",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "distributejoins",
                        description = "Whether to use distributed joins for non collocated data or not. ",
                        optional = true,
                        defaultValue = "false",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "enforcejoinorder",
                        description = "Whether to enforce join order of tables in the query or not. If set to true" +
                                " query optimizer will not reorder tables in join. ",
                        optional = true,
                        defaultValue = "false ",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "collocated",
                        description = "Whether your data is co-located or not ",
                        optional = true,
                        defaultValue = "false ",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "replicatedonly",
                        description = "Whether query contains only replicated tables or not ",
                        optional = true,
                        defaultValue = "false ",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "auto.close.server.cursor",
                        description = "Whether to close server-side cursor automatically when last piece of " +
                                "result set is retrieved or not. ",
                        optional = true,
                        defaultValue = "false",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "socket.send.buffer",
                        description = "Socket send buffer size.When set to 0, OS default will be used. ",
                        optional = true, defaultValue = "0 ",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "socket.receive.buffer",
                        description = "Socket receive buffer size.When set to 0, OS default will be used. ",
                        optional = true,
                        defaultValue = " 0",
                        type = {DataType.STRING}
                ),

                @Parameter(
                        name = "backups",
                        description = "Number of backup copies of data.",
                        optional = true,
                        defaultValue = "0",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "atomicity",
                        description = "Sets atomicity mode for the cache. ",
                        optional = true,
                        defaultValue = "atomic ",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "affinity.key",
                        description = "specifies an affinity key name which is a column of the primary key constraint.",
                        optional = true,
                        defaultValue = " column of the primary key constraint. ",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "cache.name",
                        description = "Name of the cache created. ",
                        optional = true,
                        defaultValue = " custom name of the new cache. ",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "data.region",
                        description = "Name of the data region where table entries should be stored. ",
                        optional = true,
                        defaultValue = "an existing data region name ",
                        type = {DataType.STRING}
                ),
        },
        systemParameter = {
               /*@SystemParameter(
                        name = " ",
                        description = " ",
                        defaultValue = " ",
                        possibleParameters = " "),*/
        },

        examples = {
                @Example(
                        syntax = "define stream StockStream (symbol string, price float, volume long);\n " +
                                "@Store(type=\"apacheignite\", url = \" jdbc:ignite:thin://127.0.0.1 \" ," +
                                ",auth.enabled = \"true\",username=\"ignite \", password=\" ignite ) \n" +
                                "@PrimaryKey(\"symbol\")\n" +
                                "define table StockTable (symbol string, price float, volume long);\n" +
                                "@info(name = 'query1') \n" +
                                "from StockStream\n" +
                                "insert into StockTable ; ",
                        description = "The above example creates a table in apache ignite data store if it does not " +
                                "exists already with 'symbol' as the primary key.The connection is made as specified" +
                                "by the parameters configured under '@Store' annotation.Data is inserted into table," +
                                "stockTable from stockStream"
                ),
                @Example(
                        syntax = "define stream StockStream (symbol string, price float, volume long);\n " +
                                "@Store(type=\"apacheignite\", url = \" jdbc:ignite:thin://127.0.0.1 \" ," +
                                "username=\"ignite \", password=\" ignite ) \n" +
                                "@PrimaryKey(\"symbol\")\n" +
                                "define table StockTable (symbol string, price float, volume long);\n" +
                                "@info(name = 'query2')\n " +
                                "from FooStream#window.length(1) join StockTable on " +
                                "StockTable.symbol==FooStream.name \n" +
                                "select StockTable.symbol as checkName, " +
                                "StockTable.volume as checkVolume," +
                                "StockTable.price as checkCategory\n " +
                                "insert into OutputStream;",
                        description = "The above example creates a table in apache ignite data store if it does not " +
                                "exists already with 'symbol' as the primary key.The connection is made as specified" +
                                "by the parameters configured under '@Store' annotation.Then the table is joined with" +
                                " a stream name 'FooStream' based on a condition. The following operations are " +
                                "included in the condition:\n" +
                                "[AND, OR, Comparisons(<, <=, >, >=, ==, != )]"

                )
        }
)

public class ApacheIgniteStore extends AbstractQueryableRecordTable {

    private static final Log log = LogFactory.getLog(ApacheIgniteStore.class);
    private String tableName;
    private String url;
    private String username;
    private String password;
    private String schema;
    private String authEnabled;

    private String template;
    private String distributeJoins;
    private String enforceJoinOrder;
    private String collocated;
    private String replicatedOnly;
    private String autocloseServerCursor;
    private String socketSendBuffer;
    private String socketReceiveBuffer;

    private String backups;
    private String atomicity;
    private String affinityKey;
    private String cacheName;
    private String dataRegion;
    private String binaryType;
    private boolean connected;
    private boolean isAuthEnabled;
    private boolean sslEnabled;
    private Annotation storeAnnotation;
    private Annotation primaryKey;
    private Annotation indices;
    private List<Attribute> attributes;
    private List<Integer> primaryKeyAttributePositionList;
    private Connection con;

    /**
     * Initializing the Record Table
     *
     * @param tableDefinition definintion of the table with annotations if any
     * @param configReader    this hold the {@link AbstractQueryableRecordTable} configuration reader.
     */
    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {

        storeAnnotation = AnnotationHelper.getAnnotation(ANNOTATION_STORE, tableDefinition.getAnnotations());
        indices = AnnotationHelper.getAnnotation(ANNOTATION_INDEX, tableDefinition.getAnnotations());
        primaryKey = AnnotationHelper.getAnnotation(ANNOTATION_PRIMARY_KEY, tableDefinition.getAnnotations());
        url = storeAnnotation.getElement(ANNOTATION_ELEMENT_URL);
        username = storeAnnotation.getElement(ANNOTATION_ELEMENT_USERNAME);
        password = storeAnnotation.getElement(ANNOTATION_ELEMENT_PASSWORD);
        String tableName = storeAnnotation.getElement(ANNOTATION_ELEMENT_TABLE_NAME);
        authEnabled = storeAnnotation.getElement(ANNOTATION_ELEMENT_AUTH_ENABLED);
        schema = storeAnnotation.getElement(SCHEMA);
        backups = storeAnnotation.getElement(BACKUPS);
        template = storeAnnotation.getElement(TEMPLATE);
        distributeJoins = storeAnnotation.getElement(DISTRIBUTED_JOINS);
        enforceJoinOrder = storeAnnotation.getElement(ENFORCE_JOIN_ORDER);
        collocated = storeAnnotation.getElement(COLLOCATED);
        replicatedOnly = storeAnnotation.getElement(REPLICATED_ONLY);
        autocloseServerCursor = storeAnnotation.getElement(AUTO_CLOSE_SERVER_CURSER);
        socketReceiveBuffer = storeAnnotation.getElement(SOCKET_RECEIVE_BUFFER);
        socketSendBuffer = storeAnnotation.getElement(SOCKET_SEND_BUFFER);

        atomicity = storeAnnotation.getElement(ATOMICITY);
        affinityKey = storeAnnotation.getElement(AFFINITY_KEY);
        cacheName = storeAnnotation.getElement(CACHE_NAME);
        dataRegion = storeAnnotation.getElement(DATA_REGION);

        attributes = tableDefinition.getAttributeList();
        this.tableName = ApacheIgniteTableUtils.isEmpty(tableName) ? tableDefinition.getId() : tableName;
        if (ApacheIgniteTableUtils.isEmpty(url)) {
            throw new SiddhiAppCreationException("Required parameter '" + ANNOTATION_ELEMENT_URL + " for DB " +
                    "connectivity  cannot be empty for creating table : " + this.tableName);
        }
        if (ApacheIgniteTableUtils.isEmpty(authEnabled) || authEnabled.equalsIgnoreCase("false")) {
            isAuthEnabled = false;
        } else if (authEnabled.equalsIgnoreCase("true")) {
            isAuthEnabled = true;
        }
        if (isAuthEnabled) {
            if (ApacheIgniteTableUtils.isEmpty(username)) {
                throw new SiddhiAppCreationException("Required parameter " + ANNOTATION_ELEMENT_USERNAME + " for DB " +
                        "connectivity cannot be empty for creating table : " + this.tableName +
                        " when authentication is enabled ");
            }
            if (ApacheIgniteTableUtils.isEmpty(password)) {
                throw new SiddhiAppCreationException("Required parameter " + ANNOTATION_ELEMENT_PASSWORD + " for DB " +
                        "connectivity cannot be empty for creating table : " + this.tableName + "when authentication " +
                        "is enabled ");
            }
        }
        if (primaryKey == null) {
            throw new SiddhiAppCreationException("primary key field cannot be empty for creating table : " +
                    this.tableName);
        }
        primaryKeyAttributePositionList = new ArrayList<>();
        primaryKey.getElements().forEach(element -> {
            for (int i = 0; i < this.attributes.size(); i++) {
                if (this.attributes.get(i).getName().equalsIgnoreCase(element.getValue())) {
                    primaryKeyAttributePositionList.add(i);
                }
            }
        });
    }

    /**
     * Add records to the Table
     *
     * @param records records that need to be added to the table, each Object[] represent a record and it will match
     *                the attributes of the Table Definition.
     */
    @Override
    protected void add(List<Object[]> records) throws ConnectionUnavailableException {

        PreparedStatement statement = null;
        PreparedStatement st = null;

        try {
            String insertQuery = null;
            for (Object[] record : records) {
                insertQuery = INSERT_QUERY;
                insertQuery = insertQuery.replace(COLUMNS, this.columnNames())
                        .replace(TABLE_NAME, this.tableName)
                        .replace(VALUES, this.convertAttributesValue(record));
                statement = con.prepareStatement(insertQuery);
                st = this.bindValuesToAttributes(statement, record);
            }
            log.info(insertQuery);
            if (st != null) {
                st.execute();
                st.close();
            }
        } catch (SQLException e) {
            ApacheIgniteTableUtils.cleanupConnection(null, statement, con);
            throw new ApacheIgniteTableException("Error writing to table " + this.tableName + " : " + e.getMessage(),
                    e);
        }
    }

    /**
     * Find records matching the compiled condition
     *
     * @param findConditionParameterMap map of matching StreamVariable Ids and their values corresponding to the
     *                                  compiled condition
     * @param compiledCondition         the compiledCondition against which records should be matched
     * @return RecordIterator of matching records
     */
    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap,
                                            CompiledCondition compiledCondition) throws ConnectionUnavailableException {

        ApacheIgniteCompiledCondition igniteCompiledCondition = (ApacheIgniteCompiledCondition) compiledCondition;
        String condition = igniteCompiledCondition.getCompiledQuery();
        Map<Integer, Object> constantMap = igniteCompiledCondition.getParameterConstants();
        log.info(condition);
        List<String> attri = new ArrayList<>();
        PreparedStatement st;
        StringBuilder readQuery = new StringBuilder();
        readQuery.append("SELECT ").append(" * ").append("FROM ").append(this.tableName);
        try {
            if (!condition.equals("'?'")) {
                readQuery.append(" WHERE ");

                condition = this.replaceConditionWithParameter(condition, findConditionParameterMap, constantMap);
                log.info(condition);
                readQuery.append(condition);
            }

            log.info(readQuery.toString());
            for (Attribute at : this.attributes) {
                attri.add(at.getName());
            }
            st = con.prepareStatement(readQuery.toString());
            return new ApacheIgniteIterator(con, st, st.executeQuery(), tableName, attri, this.attributes);

        } catch (SQLException e) {
            // ApacheIgniteTableUtils.cleanupConnection(rs, st, con); bug
            throw new ApacheIgniteTableException("Error retrieving records from table  " + this.tableName + " : " +
                    e.getMessage(), e);
        }
    }

    /**
     * Check if matching record exist or not
     *
     * @param containsConditionParameterMap map of matching StreamVariable Ids and their values corresponding to the
     *                                      compiled condition
     * @param compiledCondition             the compiledCondition against which records should be matched
     * @return if matching record found or not
     */
    @Override
    protected boolean contains(Map<String, Object> containsConditionParameterMap,
                               CompiledCondition compiledCondition) throws ConnectionUnavailableException {

        ApacheIgniteCompiledCondition igniteCompiledCondition = (ApacheIgniteCompiledCondition) compiledCondition;
        String condition = igniteCompiledCondition.getCompiledQuery();
        Map<Integer, Object> constantMap = igniteCompiledCondition.getParameterConstants();
        ResultSet rs = null;
        PreparedStatement st = null;
        log.info(condition);
        StringBuilder readQuery = new StringBuilder();
        readQuery.append("SELECT ").append(" * ").append("FROM ").append(this.tableName);
        try {
            if (!condition.equals("'?'")) {
                readQuery.append(" WHERE ");
                condition = this.replaceConditionWithParameter(condition, containsConditionParameterMap, constantMap);
                readQuery.append(condition);
            }

            log.info(readQuery.toString());
            st = con.prepareStatement(readQuery.toString());
            rs = st.executeQuery();
            if (rs.next()) {
                ApacheIgniteTableUtils.cleanupConnection(rs, st, con);
                return true;
            } else {
                ApacheIgniteTableUtils.cleanupConnection(rs, st, con);
                log.info("No results matching for given condition in the table : " + this.tableName);
                return false;
            }

        } catch (SQLException e) {
            ApacheIgniteTableUtils.cleanupConnection(rs, st, con);
            throw new ApacheIgniteTableException("Error performing 'contains'  on the table : '" + this.tableName +
                    "': " + e.getMessage(), e);
        }
        //return false;
    }

    /**
     * Delete all matching records
     *
     * @param deleteConditionParameterMaps map of matching StreamVariable Ids and their values corresponding to the
     *                                     compiled condition
     * @param compiledCondition            the compiledCondition against which records should be matched for deletion
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     **/
    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps, CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {

        ApacheIgniteCompiledCondition igniteCompiledCondition = (ApacheIgniteCompiledCondition) compiledCondition;
        String condition = igniteCompiledCondition.getCompiledQuery();
        log.info(condition);
        Map<Integer, Object> constantMap = igniteCompiledCondition.getParameterConstants();
        StringBuilder deleteCondition = new StringBuilder();
        PreparedStatement statement = null;
        try {
            deleteCondition.append("DELETE FROM ").append(this.tableName);
            if (!condition.equals("?")) {
                deleteCondition.append(" WHERE ");
                for (Map<String, Object> map : deleteConditionParameterMaps) {
                    condition = this.replaceConditionWithParameter(condition, map, constantMap);
                }
                deleteCondition.append(condition);
            }

            log.info(deleteCondition.toString());
            statement = con.prepareStatement(deleteCondition.toString());
            statement.execute();
            ApacheIgniteTableUtils.cleanupConnection(null, statement, con);
        } catch (SQLException e) {
            ApacheIgniteTableUtils.cleanupConnection(null, statement, con);
            throw new ApacheIgniteTableException("Error deleting records from table '" + this.tableName + "': "
                    + e.getMessage(), e);
        }
    }

    /**
     * Update all matching records
     *
     * @param compiledCondition the compiledCondition against which records should be matched for update
     * @param list              map of matching StreamVariable Ids and their values corresponding to the
     *                          compiled condition based on which the records will be updated
     * @param map               the attributes and values that should be updated if the condition matches
     * @param list1             the attributes and values that should be updated for the matching records
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    protected void update(CompiledCondition compiledCondition, List<Map<String, Object>> list,
                          Map<String, CompiledExpression> map, List<Map<String, Object>> list1)
            throws ConnectionUnavailableException {

        ApacheIgniteCompiledCondition igniteCompiledCondition = (ApacheIgniteCompiledCondition) compiledCondition;
        Map<Integer, Object> constantMap = igniteCompiledCondition.getParameterConstants();
        String condition = igniteCompiledCondition.getCompiledQuery();
        PreparedStatement statement = null;
        log.info(condition);

        try {
            for (Map<String, Object> mapCondition : list) {
                condition = this.replaceConditionWithParameter(condition, mapCondition, constantMap);
            }

            StringBuilder updateCondition = new StringBuilder();
            updateCondition.append(" UPDATE ").append(tableName).append(WHITESPACE)
                    .append("SET").append(WHITESPACE).append(this.mapAttributesWithValues(list1)).append("WHERE")
                    .append(WHITESPACE).append(condition);

            statement = con.prepareStatement(updateCondition.toString());
            statement.execute();
            ApacheIgniteTableUtils.cleanupConnection(null, statement, con);
            log.info(updateCondition);
        } catch (SQLException e) {
            ApacheIgniteTableUtils.cleanupConnection(null, statement, con);
            throw new ApacheIgniteTableException("Error performing record update operation on table '" + this.tableName
                    + "': " + e.getMessage(), e);
        }
    }

    public String mapAttributesWithValues(List<Map<String, Object>> attributeMap) {

        StringBuilder list = new StringBuilder();
        for (Map<String, Object> attributeAndValues : attributeMap) {
            for (Map.Entry<String, Object> map : attributeAndValues.entrySet()) {

                primaryKey.getElements().forEach(element -> {

                    if (!element.getValue().equalsIgnoreCase(map.getKey())) {
                        list.append(map.getKey()).append("=");
                        if (map.getValue() instanceof String) {
                            list.append("'").append(map.getValue()).append("'");
                        } else {
                            list.append(map.getValue());
                        }
                        list.append(WHITESPACE).append(",");
                    }
                });
            }
        }
        list.delete(list.length() - 1, list.length());
        return list.toString();
    }

    /**
     * Try updating the records if they exist else add the records
     *
     * @param list              map of matching StreamVariable Ids and their values corresponding to the
     *                          compiled condition based on which the records will be updated
     * @param compiledCondition the compiledCondition against which records should be matched for update
     * @param map               the attributes and values that should be updated if the condition matches
     * @param list1             the values for adding new records if the update condition did not match
     * @param list2             the attributes and values that should be updated for the matching records
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    protected void updateOrAdd(CompiledCondition compiledCondition, List<Map<String, Object>> list,
                               Map<String, CompiledExpression> map, List<Map<String, Object>> list1,
                               List<Object[]> list2) throws ConnectionUnavailableException {

        ApacheIgniteCompiledCondition igniteCompiledCondition = (ApacheIgniteCompiledCondition) compiledCondition;
        String condition = igniteCompiledCondition.getCompiledQuery();
        log.info(condition);

        try {
            StringBuilder updateCondition = new StringBuilder();
            for (Object[] record : list2) {
                updateCondition.append(" MERGE INTO ").append(this.tableName)
                        .append(" (").append(this.columnNames()).append(")").append(" values ")
                        .append("(").append(this.convertAttributesValuesToString(record)).append(")");
            }
            log.info(updateCondition);
            PreparedStatement statement = con.prepareStatement(updateCondition.toString());
            statement.execute();
            ApacheIgniteTableUtils.cleanupConnection(null, statement, con);
        } catch (SQLException e) {
            throw new ApacheIgniteTableException("Error in updating or inserting records to the table '" +
                    this.tableName + "': " + e.getMessage(), e);
        }
    }

    /**
     * Compile the matching condition
     *
     * @param expressionBuilder that helps visiting the conditions in order to compile the condition
     * @return compiled condition that can be used for matching events in find, contains, delete, update and
     * updateOrAdd
     */
    @Override
    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {

        ApacheIgniteConditionVisitor visitor = new ApacheIgniteConditionVisitor();
        expressionBuilder.build(visitor);
        return new ApacheIgniteCompiledCondition(visitor.returnCondition(), visitor.getParameters(),
                visitor.getParametersConstant());
    }

    /**
     * Compile the matching condition
     *
     * @param expressionBuilder that helps visiting the conditions in order to compile the condition
     * @return compiled condition that can be used for matching events in find, contains, delete, update and
     * updateOrAdd
     */
    @Override
    protected CompiledExpression compileSetAttribute(ExpressionBuilder expressionBuilder) {

        return compileCondition(expressionBuilder);
    }

    /**
     * This method will be called before the processing method.
     * Intention to establish connection to publish event.
     *
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    protected void connect() throws ConnectionUnavailableException {

        try {
            StringBuilder connectionParams = new StringBuilder();
            connectionParams.append(url);
            if (schema != null) {
                connectionParams.append("/").append(schema);
            }
            if (username != null) {
                connectionParams.append(SEMI_COLON).append("user").append(EQUAL).append(username);
            }
            if (password != null) {
                connectionParams.append(SEMI_COLON).append("password").append(EQUAL).append(password);
            }
            if (collocated != null) {
                connectionParams.append(SEMI_COLON).append("collocated").append(EQUAL).append(collocated);
            }
            if (distributeJoins != null) {
                connectionParams.append(SEMI_COLON).append("distributeJoins").append(EQUAL).append(distributeJoins);
            }
            if (enforceJoinOrder != null) {
                connectionParams.append(SEMI_COLON).append("enforceJoinOrder").append(EQUAL).append(enforceJoinOrder);
            }
            if (replicatedOnly != null) {
                connectionParams.append(SEMI_COLON).append("replicatedOnly").append(EQUAL).append(replicatedOnly);
            }
            if (autocloseServerCursor != null) {
                connectionParams.append(SEMI_COLON).append("autocloseServerCursor").append(EQUAL)
                        .append(autocloseServerCursor);
            }
            if (replicatedOnly != null) {
                connectionParams.append(SEMI_COLON).append("replicatedOnly").append(EQUAL).append(replicatedOnly);
            }
            if (socketSendBuffer != null) {
                connectionParams.append(SEMI_COLON).append("socketSendBuffer").append(EQUAL).append(socketSendBuffer);
            }
            if (socketReceiveBuffer != null) {
                connectionParams.append(SEMI_COLON).append("socketReceiveBuffer").append(EQUAL)
                        .append(socketReceiveBuffer);
            }

            log.info(connectionParams);
            Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
            con = DriverManager.getConnection(connectionParams.toString());
            this.createTable(storeAnnotation, primaryKey, indices);
            connected = true;
        } catch (SQLException e) {
            throw new ConnectionUnavailableException("Failed to initialize apacheIgnite store  " + e.getMessage(), e);

        } catch (ClassNotFoundException e) {
            throw new ConnectionUnavailableException("unable to find class " + e.getMessage(), e);
        }
    }

    /**
     * Called after all publishing is done, or when {@link ConnectionUnavailableException} is thrown
     * Implementation of this method should contain the steps needed to disconnect.
     */
    @Override
    protected void disconnect() {

        try {
            if (connected) {
                con.close();

            }
        } catch (SQLException e) {
            throw new ApacheIgniteTableException("unable to close connection");
        }
    }

    /**
     * The method can be called when removing an event receiver.
     * The cleanups that have to be done after removing the receiver could be done here.
     */
    @Override
    protected void destroy() {

         this.disconnect();
        try {
            if (connected) {
                con.close();
            }
        } catch (SQLException e) {
            throw new ApacheIgniteTableException("unable to close connection");
        }
    }

    @Override
    protected RecordIterator<Object[]> query(Map<String, Object> parameterMap, CompiledCondition compiledCondition,
                                             CompiledSelection compiledSelection, Attribute[] attributes)
            throws ConnectionUnavailableException {

        ApacheIgniteCompiledSelection apacheIgniteCompiledSelection = (ApacheIgniteCompiledSelection) compiledSelection;
        ApacheIgniteCompiledCondition apacheIgniteCompiledCondition = (ApacheIgniteCompiledCondition) compiledCondition;

        Map<Integer, Object> consMap = apacheIgniteCompiledCondition.getParameterConstants();
        PreparedStatement statement;
        List<String> attri;

        String query = getSelectQuery(apacheIgniteCompiledCondition, apacheIgniteCompiledSelection,
                parameterMap, consMap);

        attri = apacheIgniteCompiledSelection.getSelectedAttributes();
        log.info(query);
        try {
            statement = con.prepareStatement(query);
        } catch (SQLException e) {
            throw new ApacheIgniteTableException("Error when preparing to execute query " +
                    "in table :" + this.tableName + e.getMessage());
        }
        try {
            return new ApacheIgniteIterator(con, statement, statement.executeQuery(),
                    this.tableName, attri, this.attributes);
        } catch (SQLException e) {
            throw new ApacheIgniteTableException("unable to execute query " + e.getMessage());
        }
    }

    private String getSelectQuery(ApacheIgniteCompiledCondition apacheIgniteCompiledCondition,
                                  ApacheIgniteCompiledSelection apacheIgniteCompiledSelection,
                                  Map<String, Object> parameterMap, Map<Integer, Object> constantMap) {

        String selectors = apacheIgniteCompiledSelection.getCompiledSelectClause().getCompiledQuery();
        log.info(selectors);

        String condition = apacheIgniteCompiledCondition.getCompiledQuery();
        StringBuilder selectQuery = new StringBuilder("select").append(WHITESPACE);
        selectQuery.append(selectors).append(WHITESPACE)
                .append("from").append(WHITESPACE).append(this.tableName).append(WHITESPACE);
        if (!condition.equals("'*'")) {
            selectQuery.append("where").append(WHITESPACE);
            for (Map.Entry<String, Object> entry : parameterMap.entrySet()) {
                Object streamVariable = entry.getValue();
                if (streamVariable instanceof String) {
                    condition = condition.replaceFirst(Pattern.quote("?"), streamVariable.toString());
                } else {
                    condition = condition.replaceFirst(Pattern.quote("'?'"), streamVariable.toString());
                }
            }
            for (Map.Entry<Integer, Object> entry : constantMap.entrySet()) {
                Object constant = entry.getValue();
                if (constant instanceof String) {
                    condition = condition.replaceFirst(Pattern.quote("*"), constant.toString());
                } else {
                    condition = condition.replaceFirst(Pattern.quote("'*'"), constant.toString());
                }
            }
            selectQuery.append(condition);
        }
        log.info(selectQuery.toString());
        ApacheIgniteCompiledCondition compileGroupByClause = apacheIgniteCompiledSelection.getCompiledGroupByClause();
        if (compileGroupByClause != null) {
            String groupByClause = " group by " + compileGroupByClause.getCompiledQuery();
            selectQuery.append(WHITESPACE).append(groupByClause);
        }
        ApacheIgniteCompiledCondition compileOrderByClause = apacheIgniteCompiledSelection.getCompiledOrderByClause();
        if (compileOrderByClause != null) {
            String orderByClause = " order by " + compileOrderByClause.getCompiledQuery();
            selectQuery.append(WHITESPACE).append(orderByClause);
        }
        if (apacheIgniteCompiledSelection.getLimit() != null) {
            selectQuery.append(" limit ").append(apacheIgniteCompiledSelection.getLimit());
        }
        if (apacheIgniteCompiledSelection.getOffset() != null) {
            selectQuery.append(" offset ").append(apacheIgniteCompiledSelection.getOffset());
        }
        ApacheIgniteCompiledCondition compileHavingClause = apacheIgniteCompiledSelection.getCompiledHavingClause();
        if (compileHavingClause != null) {
            String havingClause = " having " + compileHavingClause.getCompiledQuery();
            selectQuery.append(WHITESPACE).append(havingClause);
        }

        return selectQuery.toString();
    }

    @Override
    protected CompiledSelection compileSelection(List<SelectAttributeBuilder> selectAttributeBuilders,
                                                 List<ExpressionBuilder> groupByExpressionBuilder,
                                                 ExpressionBuilder havingExpressionBuilder,
                                                 List<OrderByAttributeBuilder> orderByAttributeBuilders,
                                                 Long limit, Long offset) {

        return new ApacheIgniteCompiledSelection(

                compileSelectClause(selectAttributeBuilders),
                (groupByExpressionBuilder == null) ? null : compileClause(groupByExpressionBuilder),
                (havingExpressionBuilder == null) ? null :
                        compileClause(Collections.singletonList(havingExpressionBuilder)),
                (orderByAttributeBuilders == null) ? null : compileOrderByClause(orderByAttributeBuilders),
                limit, offset, getSelectList(selectAttributeBuilders));
    }

    private ApacheIgniteCompiledCondition compileSelectClause(List<SelectAttributeBuilder> selectAttributeBuilders) {

        StringBuilder compiledSelectionList = new StringBuilder();
        SortedMap<Integer, Object> paramMap = new TreeMap<>();
        SortedMap<Integer, Object> paramConstMap = new TreeMap<>();

        int offset = 0;

        for (SelectAttributeBuilder selectAttributeBuilder : selectAttributeBuilders) {
            ApacheIgniteConditionVisitor visitor = new ApacheIgniteConditionVisitor();
            selectAttributeBuilder.getExpressionBuilder().build(visitor);

            String compiledCondition = visitor.returnCondition();
            log.info(compiledCondition);
            compiledSelectionList.append(compiledCondition);
            if (selectAttributeBuilder.getRename() != null && !selectAttributeBuilder.getRename().isEmpty()) {

                compiledSelectionList.append(WHITESPACE).append("AS").append(WHITESPACE)
                        .append(selectAttributeBuilder.getRename());
            }
            compiledSelectionList.append(SEPARATOR);
            Map<Integer, Object> conditionParamMap = visitor.getParameters();
            int maxOrdinal = 0;
            for (Map.Entry<Integer, Object> entry : conditionParamMap.entrySet()) {
                Integer ordinal = entry.getKey();
                paramMap.put(ordinal + offset, entry.getValue());
                if (ordinal > maxOrdinal) {
                    maxOrdinal = ordinal;
                }
            }
            offset = maxOrdinal;

        }
        if (compiledSelectionList.length() > 0) {
            compiledSelectionList.setLength(compiledSelectionList.length() - 1); //remove last comma
        }
        return new ApacheIgniteCompiledCondition(compiledSelectionList.toString(), paramMap, paramConstMap);
    }

    private List<String> getSelectList(List<SelectAttributeBuilder> selectAttributeBuilders) {

        List<String> attri = new ArrayList<>();
        for (SelectAttributeBuilder selectAttributeBuilder : selectAttributeBuilders) {
            ApacheIgniteConditionVisitor visitor = new ApacheIgniteConditionVisitor();
            selectAttributeBuilder.getExpressionBuilder().build(visitor);

            String compiledCondition = visitor.returnCondition();
            log.info(compiledCondition);
            attri.add(compiledCondition);
        }
        return attri;
    }

    private ApacheIgniteCompiledCondition compileClause(List<ExpressionBuilder> expressionBuilders) {

        StringBuilder compiledSelectionList = new StringBuilder();
        SortedMap<Integer, Object> paramMap = new TreeMap<>();
        SortedMap<Integer, Object> paramCons = new TreeMap<>();

        int offset = 0;
        for (ExpressionBuilder expressionBuilder : expressionBuilders) {
            ApacheIgniteConditionVisitor visitor = new ApacheIgniteConditionVisitor();
            expressionBuilder.build(visitor);
            String compiledCondition = visitor.returnCondition();
            compiledSelectionList.append(compiledCondition).append(SEPARATOR);
            Map<Integer, Object> conditionParamMap = visitor.getParameters();
            Map<Integer, Object> conditionConstMap = visitor.getParametersConstant();
            int maxOrdinal = 0;
            for (Map.Entry<Integer, Object> entry : conditionParamMap.entrySet()) {
                Integer ordinal = entry.getKey();
                paramMap.put(ordinal + offset, entry.getValue());
                if (ordinal > maxOrdinal) {
                    maxOrdinal = ordinal;
                }
            }
            for (Map.Entry<Integer, Object> entry : conditionConstMap.entrySet()) {
                Integer ordinal = entry.getKey();
                paramCons.put(ordinal + offset, entry.getValue());
                if (ordinal > maxOrdinal) {
                    maxOrdinal = ordinal;
                }
            }
            offset = maxOrdinal;
        }

        if (compiledSelectionList.length() > 0) {
            compiledSelectionList.setLength(compiledSelectionList.length() - 1);
        }
        String condition = compiledSelectionList.toString();
        for (Map.Entry<Integer, Object> map : paramCons.entrySet()) {
            Object constant = map.getValue();
            if (constant instanceof String) {
                condition = condition.replaceFirst(Pattern.quote("*"), map.getValue().toString());
            } else {
                condition = condition.replaceFirst(Pattern.quote("'*'"), map.getValue().toString());
            }
        }

        return new ApacheIgniteCompiledCondition(condition, paramMap, paramCons);
    }

    private ApacheIgniteCompiledCondition compileOrderByClause(List<OrderByAttributeBuilder> orderByAttributeBuilders) {

        StringBuilder compiledSelectionList = new StringBuilder();
        SortedMap<Integer, Object> paramMap = new TreeMap<>();
        int offset = 0;
        for (OrderByAttributeBuilder orderByAttributeBuilder : orderByAttributeBuilders) {
            ApacheIgniteConditionVisitor visitor = new ApacheIgniteConditionVisitor();
            orderByAttributeBuilder.getExpressionBuilder().build(visitor);
            String compiledCondition = visitor.returnCondition();
            compiledSelectionList.append(compiledCondition);
            OrderByAttribute.Order order = orderByAttributeBuilder.getOrder();
            if (order == null) {
                compiledSelectionList.append(SEPARATOR);
            } else {
                compiledSelectionList.append(WHITESPACE).append(order.toString()).append(SEPARATOR);
            }
            Map<Integer, Object> conditionParamMap = visitor.getParameters();
            int maxOrdinal = 0;
            for (Map.Entry<Integer, Object> entry : conditionParamMap.entrySet()) {
                Integer ordinal = entry.getKey();
                paramMap.put(ordinal + offset, entry.getValue());
                if (ordinal > maxOrdinal) {
                    maxOrdinal = ordinal;
                }
            }
            offset = maxOrdinal;
        }
        if (compiledSelectionList.length() > 0) {
            compiledSelectionList.setLength(compiledSelectionList.length() - 1);
        }
        return new ApacheIgniteCompiledCondition(compiledSelectionList.toString(), paramMap, null);
    }

    public void createTable(Annotation store, Annotation primaryKey, Annotation indices) throws SQLException {

        PreparedStatement sts = null;
        try {
            StringBuilder tableCreateQuery = new StringBuilder();
            List<Element> primaryKeyList = (primaryKey == null) ? new ArrayList<>() : primaryKey.getElements();
            tableCreateQuery.append(TABLE_CREATE_QUERY).append(this.tableName).append(OPEN_PARENTHESIS);
            this.attributes.forEach(attribute -> {
                tableCreateQuery.append(attribute.getName()).append(WHITESPACE);
                switch (attribute.getType()) {
                    case BOOL:
                        tableCreateQuery.append(BOOLEAN).append(WHITESPACE);
                        break;
                    case DOUBLE:
                        tableCreateQuery.append("double ");
                        break;
                    case FLOAT:
                        tableCreateQuery.append(FLOAT).append(WHITESPACE);
                        break;
                    case INT:
                        tableCreateQuery.append("integer ");
                        break;
                    case LONG:
                        tableCreateQuery.append(LONG);
                        break;
                    case OBJECT:
                        tableCreateQuery.append(binaryType);
                        break;
                    case STRING:
                        tableCreateQuery.append(STRING);
                        break;
                }
                tableCreateQuery.append(SEPARATOR);
            });
            tableCreateQuery.append(SQL_PRIMARY_KEY_DEF).append(OPEN_PARENTHESIS).
                    append(this.flattenAnnotatedElements(primaryKeyList))
                    .append(CLOSE_PARENTHESIS);
            tableCreateQuery.append(CLOSE_PARENTHESIS);
            tableCreateQuery.append(" WITH ").append("\"template").append(EQUAL);

            if (template != null) {
                tableCreateQuery.append(template).append("\"");
            } else {
                tableCreateQuery.append("PARTITIONED").append("\"");
            }

            if (backups != null) {
                tableCreateQuery.append(SEPARATOR).append("\"").append("Backups").append(EQUAL).append(backups)
                        .append("\"");
            }
            if (atomicity != null) {
                tableCreateQuery.append(SEPARATOR).append("\"").append("Atomicity").append(EQUAL).append(atomicity)
                        .append("\"");
            }
            if (affinityKey != null) {
                tableCreateQuery.append(SEPARATOR).append("\"").append("affinity_Key").append(EQUAL)
                        .append(affinityKey).append("\"");
            }
            if (cacheName != null) {
                tableCreateQuery.append(SEPARATOR).append("\"").append("Cache_name").append(EQUAL).append(cacheName)
                        .append("\"");
            }
            if (dataRegion != null) {
                tableCreateQuery.append(SEPARATOR).append("\"").append("Data_region").append(EQUAL).append(dataRegion)
                        .append("\"");
            }

            log.info(tableCreateQuery);
            sts = con.prepareStatement(tableCreateQuery.toString());
            sts.execute();
            ApacheIgniteTableUtils.cleanupConnection(null, sts, con);

        } catch (SQLException e) {
            ApacheIgniteTableUtils.cleanupConnection(null, sts, con);
            throw new ApacheIgniteTableException("table creation failed " + e.getMessage(), e);
        }
    }

    private String columnNames() {

        StringBuilder columns = new StringBuilder();
        for (int i = 0; i < attributes.size(); i++) {
            columns.append(attributes.get(i).getName()).append(" ").append(",");
        }
        columns.delete(columns.length() - 2, columns.length());
        log.info(columns.toString());
        return columns.toString();
    }

    private String replaceConditionWithParameter(String condition,
                                                 Map<String, Object> conditionParameterMap
            , Map<Integer, Object> constantMap) {

        for (Map.Entry<String, Object> map : conditionParameterMap.entrySet()) {

            Object streamVariable = map.getValue();
            if (streamVariable instanceof String) {
                condition = condition.replaceFirst(Pattern.quote("?"), map.getValue().toString());
            } else {
                condition = condition.replaceFirst(Pattern.quote("'?'"), map.getValue().toString());
            }
        }

        for (Map.Entry<Integer, Object> map : constantMap.entrySet()) {

            Object constant = map.getValue();
            if (constant instanceof String) {
                condition = condition.replaceFirst(Pattern.quote("*"), map.getValue().toString());
            } else {
                condition = condition.replaceFirst(Pattern.quote("'*'"), map.getValue().toString());
            }
        }

        return condition;
    }

    //convert record to comma separated string
    private String convertAttributesValuesToString(Object[] record) {

        StringBuilder values = new StringBuilder();
        for (int i = 0; i < record.length; i++) {
            if (record[i] instanceof String) {
                values.append("'");
                values.append(record[i].toString()).append("'").append(",");
            } else {
                values.append(record[i].toString()).append(" ").append(",");
            }
        }
        values.delete(values.length() - 2, values.length());
        return values.toString();
    }

    private String convertAttributesValue(Object[] record) {

        StringBuilder values = new StringBuilder();
        for (int i = 0; i < record.length; i++) {
            values.append("?").append(" ").append(",");

        }
        values.delete(values.length() - 2, values.length());
        return values.toString();
    }

    private PreparedStatement bindValuesToAttributes(PreparedStatement sql, Object[] record) {

        try {
            for (int i = 0; i < attributes.size(); i++) {

                Attribute attribute = attributes.get(i);
                switch (attribute.getType()) {
                    case BOOL:
                        sql.setBoolean(i + 1, Boolean.parseBoolean(record[i].toString()));
                        break;
                    case STRING:
                        sql.setString(i + 1, record[i].toString());
                        break;
                    case LONG:
                        sql.setLong(i + 1, Long.parseLong(record[i].toString()));
                        break;
                    case FLOAT:
                        sql.setFloat(i + 1, Float.parseFloat(record[i].toString()));
                        break;
                    case OBJECT:
                        sql.setObject(i + 1, record[i]);
                        break;
                    case INT:
                        sql.setInt(i + 1, Integer.parseInt(record[i].toString()));
                        break;
                    case DOUBLE:
                        sql.setDouble(i + 1, Double.parseDouble(record[i].toString()));

                }
            }
            return sql;
        } catch (SQLException e) {
            throw new ApacheIgniteTableException("unable to insert " + e.getMessage());
        }
    }

    //convert list of elements to a comma separated string.
    public String flattenAnnotatedElements(List<Element> elements) {

        StringBuilder sb = new StringBuilder();
        elements.forEach(elem -> {
            sb.append(elem.getValue());
            if (elements.indexOf(elem) != elements.size() - 1) {
                sb.append(",");
            }
        });
        return sb.toString();
    }

}

