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

import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.ANNOTATION_ELEMENT_PASSWORD;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.ANNOTATION_ELEMENT_URL;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.ANNOTATION_ELEMENT_USERNAME;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.CLOSE_PARENTHESIS;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.DELETE;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.DOUBLE_QUOTES;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.EQUAL;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.FROM;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.GROUP_BY;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.HAVING;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.IGNITE_AFFINITY_KEY;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.IGNITE_ATOMICITY;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.IGNITE_AUTO_CLOSE_SERVER_CURSER;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.IGNITE_BACKUPS;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.IGNITE_CACHE_NAME;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.IGNITE_COLLOCATED;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.IGNITE_DATA_REGION;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.IGNITE_DISTRIBUTE_JOINS;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.IGNITE_ENFORCE_JOIN_ORDER;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.IGNITE_PARTITIONED;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.IGNITE_PASSWORD;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.IGNITE_REPLICATED_ONLY;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.IGNITE_SOCKET_RECEIVE_BUFFER;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.IGNITE_SOCKET_SEND_BUFFER;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.IGNITE_TEMPLATE;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.IGNITE_USER;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.LIMIT;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.MERGE;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.OFFSET;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.OPEN_PARENTHESIS;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.ORDER_BY;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.SELECT;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.SEMICOLON;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.SEPARATOR;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.SET;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.SQL_PRIMARY_KEY_DEF;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.TABLE_CREATE_QUERY;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.UPDATE;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.VALUE;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.WHERE;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.WHITESPACE;
import static org.wso2.extension.siddhi.store.ignite.ApacheIgniteConstants.WITH;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_PRIMARY_KEY;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_STORE;

/**
 * Class representing Apache Ignite store implementation.
 */
@Extension(
        name = "apacheignite",
        namespace = "store",
        description = "This extension connects to apache Ignite store." +
                "It also implements read-write operations on connected apache ignite data store.",
        parameters = {
                @Parameter(
                        name = "url",
                        description = "Describes the url required for establishing the connection with apache ignite" +
                                "store. ",
                        type = {DataType.STRING}
                ),
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
                @Parameter(name = "table.name",
                        description = "The name with which the Siddhi store must be persisted in the Apache Ignite " +
                                "store.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "The table name defined in the Siddhi Application query."),
                @Parameter(
                        name = "schema",
                        description = "Schema name to access.Possible values for defining schema are public,ignite " +
                                "and any custom schema defined by user. ",
                        optional = true,
                        defaultValue = "Public",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "template",
                        description = " name of a cache template registered in Ignite to use as a configuration " +
                                "for the distributed cache.The possible values are partitioned and replicated. ",
                        optional = true,
                        defaultValue = "partitioned ",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "distribute.joins",
                        description = "Whether to use distributed joins for non collocated data or not. ",
                        optional = true,
                        defaultValue = "false",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "enforce.join.order",
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
                        defaultValue = "false",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "replicated.only",
                        description = "Whether query contains only replicated tables or not ",
                        optional = true,
                        defaultValue = "false",
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
                        defaultValue = "0",
                        type = {DataType.STRING}
                ),

                @Parameter(
                        name = "backups",
                        description = "Number of backup copies of data.It can take the value of any positive integer",
                        optional = true,
                        defaultValue = "0",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "atomicity",
                        description = "Sets atomicity mode for the cache.The possible values for atomicity are " +
                                "atomic,transactional and transactional_snapshot. ",
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
                        description = "Name of the cache created.It can take any custom name defined by user and " +
                                "default cache name takes the format {schema}_SQL_{table.name}",
                        optional = true,
                        defaultValue = " default name of the new cache. ",
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
    private StringBuilder connectionParams;
    private boolean connected;
    private boolean isAuthEnabled;
    private Annotation storeAnnotation;
    private Annotation primaryKey;
    private List<Attribute> attributes;
    private Connection connection;

    /**
     * Initializing the Record Table
     *
     * @param tableDefinition definintion of the table with annotations if any
     * @param configReader    this hold the {@link AbstractQueryableRecordTable} configuration reader.
     */
    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {

        storeAnnotation = AnnotationHelper.getAnnotation(ANNOTATION_STORE, tableDefinition.getAnnotations());
        primaryKey = AnnotationHelper.getAnnotation(ANNOTATION_PRIMARY_KEY, tableDefinition.getAnnotations());
        url = storeAnnotation.getElement(ANNOTATION_ELEMENT_URL);
        username = storeAnnotation.getElement(ANNOTATION_ELEMENT_USERNAME);
        password = storeAnnotation.getElement(ANNOTATION_ELEMENT_PASSWORD);
        String tableName = storeAnnotation.getElement(ApacheIgniteConstants.ANNOTATION_ELEMENT_TABLE_NAME);
        String authEnabled = storeAnnotation.getElement(ApacheIgniteConstants.ANNOTATION_ELEMENT_AUTH_ENABLED);
        schema = storeAnnotation.getElement(ApacheIgniteConstants.SCHEMA);
        backups = storeAnnotation.getElement(ApacheIgniteConstants.BACKUPS);
        template = storeAnnotation.getElement(ApacheIgniteConstants.TEMPLATE);
        distributeJoins = storeAnnotation.getElement(ApacheIgniteConstants.DISTRIBUTE_JOINS);
        enforceJoinOrder = storeAnnotation.getElement(ApacheIgniteConstants.ENFORCE_JOIN_ORDER);
        collocated = storeAnnotation.getElement(ApacheIgniteConstants.COLLOCATED);
        replicatedOnly = storeAnnotation.getElement(ApacheIgniteConstants.REPLICATED_ONLY);
        autocloseServerCursor = storeAnnotation.getElement(ApacheIgniteConstants.AUTO_CLOSE_SERVER_CURSER);
        socketReceiveBuffer = storeAnnotation.getElement(ApacheIgniteConstants.SOCKET_RECEIVE_BUFFER);
        socketSendBuffer = storeAnnotation.getElement(ApacheIgniteConstants.SOCKET_SEND_BUFFER);
        atomicity = storeAnnotation.getElement(ApacheIgniteConstants.ATOMICITY);
        affinityKey = storeAnnotation.getElement(ApacheIgniteConstants.AFFINITY_KEY);
        cacheName = storeAnnotation.getElement(ApacheIgniteConstants.CACHE_NAME);
        dataRegion = storeAnnotation.getElement(ApacheIgniteConstants.DATA_REGION);
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
                    this.tableName + " in ignite store ");
        }
        connectionParams = new StringBuilder();
        connectionParams.append(url);
        if (schema != null && !schema.isEmpty()) {
            connectionParams.append(ApacheIgniteConstants.SLASH).append(schema);
        }
        if (username != null && !username.isEmpty()) {
            connectionParams.append(SEMICOLON).append(IGNITE_USER).append(EQUAL).append(username);
        }
        if (password != null && !password.isEmpty()) {
            connectionParams.append(SEMICOLON).append(IGNITE_PASSWORD).append(EQUAL).append(password);
        }
        if (collocated != null && !collocated.isEmpty()) {
            connectionParams.append(SEMICOLON).append(IGNITE_COLLOCATED).append(EQUAL).append(collocated);
        }
        if (distributeJoins != null && !distributeJoins.isEmpty()) {
            connectionParams.append(SEMICOLON).append(IGNITE_DISTRIBUTE_JOINS).append(EQUAL)
                    .append(distributeJoins);
        }
        if (enforceJoinOrder != null && !enforceJoinOrder.isEmpty()) {
            connectionParams.append(SEMICOLON).append(IGNITE_ENFORCE_JOIN_ORDER).append(EQUAL)
                    .append(enforceJoinOrder);
        }
        if (replicatedOnly != null && !replicatedOnly.isEmpty()) {
            connectionParams.append(SEMICOLON).append(IGNITE_REPLICATED_ONLY).append(EQUAL).append(replicatedOnly);
        }
        if (autocloseServerCursor != null && !autocloseServerCursor.isEmpty()) {
            connectionParams.append(SEMICOLON).append(IGNITE_AUTO_CLOSE_SERVER_CURSER).append(EQUAL)
                    .append(autocloseServerCursor);
        }
        if (replicatedOnly != null && !replicatedOnly.isEmpty()) {
            connectionParams.append(SEMICOLON).append(IGNITE_REPLICATED_ONLY).append(EQUAL).append(replicatedOnly);
        }
        if (socketSendBuffer != null && !socketSendBuffer.isEmpty()) {
            connectionParams.append(SEMICOLON).append(IGNITE_SOCKET_SEND_BUFFER).append(EQUAL)
                    .append(socketSendBuffer);
        }
        if (socketReceiveBuffer != null && !socketReceiveBuffer.isEmpty()) {
            connectionParams.append(SEMICOLON).append(IGNITE_SOCKET_RECEIVE_BUFFER).append(EQUAL)
                    .append(socketReceiveBuffer);
        }
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
        try {
            String insertQuery;
            for (Object[] record : records) {
                insertQuery = ApacheIgniteConstants.INSERT_QUERY;
                insertQuery = insertQuery.replace(ApacheIgniteConstants.COLUMNS, this.columnNames())
                        .replace(ApacheIgniteConstants.TABLE_NAME, this.tableName)
                        .replace(ApacheIgniteConstants.VALUES, this.convertAttributesValuesToString(record));
                statement = connection.prepareStatement(insertQuery);
            }
            if (statement != null) {
                statement.execute();
                statement.close();
            }
        } catch (SQLException e) {
            if (e.getSQLState().equals("08006")) {
                throw new ConnectionUnavailableException("Failed to write to table : " + this.tableName + " in " +
                        "ignite store  : " + e.getMessage(), e);
            }
            throw new ApacheIgniteTableException("Error writing to table " + this.tableName + " in ignite store " +
                    " : " + e.getMessage(), e);
        } finally {
            ApacheIgniteTableUtils.cleanup(null, statement);
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
                                            CompiledCondition compiledCondition) {

        ApacheIgniteCompiledCondition igniteCompiledCondition = (ApacheIgniteCompiledCondition) compiledCondition;
        String condition = igniteCompiledCondition.getCompiledQuery();
        Map<Integer, Object> constantMap = igniteCompiledCondition.getParameterConstants();
        List<String> attributeList = new ArrayList<>();
        PreparedStatement statement;
        ResultSet resultSet;
        StringBuilder readQuery = new StringBuilder();
        readQuery.append(SELECT).append(WHITESPACE).append("*").append(WHITESPACE).append(FROM)
                .append(WHITESPACE).append(this.tableName);
        try {
            if (!condition.equals("'?'")) {
                readQuery.append(WHITESPACE).append(WHERE).append(WHITESPACE);
                condition = this.replaceConditionWithParameter(condition, findConditionParameterMap, constantMap);
                readQuery.append(condition);
            }
            for (Attribute attribute : this.attributes) {
                attributeList.add(attribute.getName());
            }
            statement = connection.prepareStatement(readQuery.toString());
            resultSet = statement.executeQuery();
            return new ApacheIgniteIterator(statement, resultSet, this.tableName,
                    attributeList, this.attributes);
        } catch (SQLException e) {
            throw new ApacheIgniteTableException("Error retrieving records from the table  " + this.tableName +
                    " in ignite store : " + e.getMessage(), e);
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
                               CompiledCondition compiledCondition) {

        ApacheIgniteCompiledCondition igniteCompiledCondition = (ApacheIgniteCompiledCondition) compiledCondition;
        String condition = igniteCompiledCondition.getCompiledQuery();
        Map<Integer, Object> constantMap = igniteCompiledCondition.getParameterConstants();
        ResultSet rs = null;
        PreparedStatement statement = null;
        StringBuilder readQuery = new StringBuilder();
        readQuery.append(SELECT).append(WHITESPACE).append("*").append(WHITESPACE).append(FROM).append(WHITESPACE)
                .append(this.tableName);
        try {
            if (!condition.equals("'?'")) {
                readQuery.append(WHITESPACE).append(WHERE).append(WHITESPACE);
                condition = this.replaceConditionWithParameter(condition, containsConditionParameterMap, constantMap);
                readQuery.append(condition);
            }
            statement = connection.prepareStatement(readQuery.toString());
            rs = statement.executeQuery();
            return rs.next();
        } catch (SQLException e) {
            throw new ApacheIgniteTableException("Error performing 'contains'  on the table : '" + this.tableName +
                    "' in ignite store : " + e.getMessage(), e);
        } finally {
            ApacheIgniteTableUtils.cleanup(rs, statement);
        }
    }

    /**
     * Delete all matching records
     *
     * @param deleteConditionParameterMaps map of matching StreamVariable Ids and their values corresponding to the
     *                                     compiled condition
     * @param compiledCondition            the compiledCondition against which records should be matched for deletion
     **/
    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps, CompiledCondition compiledCondition) {

        ApacheIgniteCompiledCondition igniteCompiledCondition = (ApacheIgniteCompiledCondition) compiledCondition;
        String condition = igniteCompiledCondition.getCompiledQuery();
        Map<Integer, Object> constantMap = igniteCompiledCondition.getParameterConstants();
        StringBuilder deleteCondition = new StringBuilder();
        PreparedStatement statement = null;
        try {
            deleteCondition.append(DELETE).append(WHITESPACE).append(FROM).append(WHITESPACE).append(this.tableName);
            if (!condition.equals("?")) {
                deleteCondition.append(WHITESPACE).append(WHERE).append(WHITESPACE);
                for (Map<String, Object> map : deleteConditionParameterMaps) {
                    condition = this.replaceConditionWithParameter(condition, map, constantMap);
                }
                deleteCondition.append(condition);
            }
            statement = connection.prepareStatement(deleteCondition.toString());
            statement.execute();
        } catch (SQLException e) {
            throw new ApacheIgniteTableException("Error deleting records from table '" + this.tableName + "' in " +
                    "ignite store: " + e.getMessage(), e);
        } finally {
            ApacheIgniteTableUtils.cleanup(null, statement);
        }
    }

    /**
     * Update all matching records
     *
     * @param compiledCondition the compiledCondition against which records should be matched for update
     * @param list              map of matching StreamVariable Ids and their values corresponding to the
     *                          compiled condition based on which the records will be updated
     * @param map               the attributes and values that should be updated if the condition matches
     * @param listToBeUpdated   the attributes and values that should be updated for the matching records
     */
    @Override
    protected void update(CompiledCondition compiledCondition, List<Map<String, Object>> list,
                          Map<String, CompiledExpression> map, List<Map<String, Object>> listToBeUpdated) {

        ApacheIgniteCompiledCondition igniteCompiledCondition = (ApacheIgniteCompiledCondition) compiledCondition;
        Map<Integer, Object> constantMap = igniteCompiledCondition.getParameterConstants();
        String condition = igniteCompiledCondition.getCompiledQuery();
        PreparedStatement statement = null;
        try {
            for (Map<String, Object> mapCondition : list) {
                condition = this.replaceConditionWithParameter(condition, mapCondition, constantMap);
            }
            StringBuilder updateCondition = new StringBuilder();
            updateCondition.append(UPDATE).append(WHITESPACE).append(this.tableName).append(WHITESPACE)
                    .append(SET).append(WHITESPACE).append(this.mapAttributesWithValues(listToBeUpdated))
                    .append(WHERE).append(WHITESPACE).append(condition);
            statement = connection.prepareStatement(updateCondition.toString());
            statement.execute();
        } catch (SQLException e) {
            throw new ApacheIgniteTableException("Error performing record update operation on table '" + this.tableName
                    + "' in ignite store : " + e.getMessage(), e);
        } finally {
            ApacheIgniteTableUtils.cleanup(null, statement);
        }
    }

    /**
     * append  non primary key attributes and their corresponding values to a string.
     *
     * @param attributeMap MAp of attributes and values
     * @return a string which contain attributes and their corresponding values
     */
    private String mapAttributesWithValues(List<Map<String, Object>> attributeMap) {

        StringBuilder list = new StringBuilder();
        for (Map<String, Object> attributeAndValues : attributeMap) {
            for (Map.Entry<String, Object> map : attributeAndValues.entrySet()) {
                primaryKey.getElements().forEach(element -> {
                    if (!element.getValue().equalsIgnoreCase(map.getKey())) {
                        list.append(map.getKey()).append(EQUAL);
                        if (map.getValue() instanceof String) {
                            list.append("'").append(map.getValue()).append("'");
                        } else {
                            list.append(map.getValue());
                        }
                        list.append(WHITESPACE).append(SEPARATOR);
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
     * @param list               map of matching StreamVariable Ids and their values corresponding to the
     *                           compiled condition based on which the records will be updated
     * @param compiledCondition  the compiledCondition against which records should be matched for update
     * @param map                the attributes and values that should be updated if the condition matches
     * @param list1              the values for adding new records if the update condition did not match
     * @param updateOrInsertList the attributes and values that should be updated for the matching records
     */
    @Override
    protected void updateOrAdd(CompiledCondition compiledCondition, List<Map<String, Object>> list,
                               Map<String, CompiledExpression> map, List<Map<String, Object>> list1,
                               List<Object[]> updateOrInsertList) {

        PreparedStatement statement = null;
        try {
            StringBuilder updateCondition = new StringBuilder();
            for (Object[] record : updateOrInsertList) {
                updateCondition.append(MERGE).append(WHITESPACE).append(this.tableName).append(WHITESPACE)
                        .append(OPEN_PARENTHESIS).append(this.columnNames()).append(CLOSE_PARENTHESIS)
                        .append(WHITESPACE)
                        .append(VALUE).append(WHITESPACE)
                        .append(OPEN_PARENTHESIS).append(this.convertAttributesValuesToString(record))
                        .append(CLOSE_PARENTHESIS);
            }
            statement = connection.prepareStatement(updateCondition.toString());
            statement.execute();
        } catch (SQLException e) {
            throw new ApacheIgniteTableException("Error in updating or inserting records to the table '" +
                    this.tableName + "' in ignite store : " + e.getMessage(), e);
        } finally {
            ApacheIgniteTableUtils.cleanup(null, statement);
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
            Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
            connection = DriverManager.getConnection(connectionParams.toString());
            this.createTable(primaryKey);
            connected = true;
        } catch (SQLException e) {
            if (e.getSQLState().equals("08004")) {
                throw new ApacheIgniteTableException("Either provided  username : " + username +
                        " or password is incorrect for connecting to ignite store " + e.getMessage(), e);
            }
            throw new ConnectionUnavailableException("Failed to initialize Ignite store  " + e.getMessage(), e);
        } catch (ClassNotFoundException e) {
            throw new ApacheIgniteTableException("unable to find class IgniteJdbcThinDriver" + e.getMessage(), e);
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
                connection.close();
            }
        } catch (SQLException e) {
            throw new ApacheIgniteTableException("unable to close the connection for ignite store.");
        }
    }

    /**
     * The method can be called when removing an event receiver.
     * The cleanups that have to be done after removing the receiver could be done here.
     */
    @Override
    protected void destroy() {

        this.disconnect();
    }

    @Override
    protected RecordIterator<Object[]> query(Map<String, Object> parameterMap, CompiledCondition compiledCondition,
                                             CompiledSelection compiledSelection, Attribute[] attributes) {

        ApacheIgniteCompiledSelection apacheIgniteCompiledSelection = (ApacheIgniteCompiledSelection) compiledSelection;
        ApacheIgniteCompiledCondition apacheIgniteCompiledCondition = (ApacheIgniteCompiledCondition) compiledCondition;
        Map<Integer, Object> constantMap = apacheIgniteCompiledCondition.getParameterConstants();
        PreparedStatement statement;
        List<String> attributeList;
        String query = getSelectQuery(apacheIgniteCompiledCondition, apacheIgniteCompiledSelection,
                parameterMap, constantMap);
        attributeList = apacheIgniteCompiledSelection.getSelectedAttributes();
        try {
            statement = connection.prepareStatement(query);
        } catch (SQLException e) {
            throw new ApacheIgniteTableException("Error when preparing to execute query " +
                    "in table :" + this.tableName + " in ignite store " + e.getMessage());
        }
        try {
            return new ApacheIgniteIterator(statement, statement.executeQuery(),
                    this.tableName, attributeList, this.attributes);
        } catch (SQLException e) {
            throw new ApacheIgniteTableException("unable to execute query for table :" + this.tableName + " in " +
                    "ignite store : " + e.getMessage());
        }
    }

    /**
     * Compose store query
     *
     * @param apacheIgniteCompiledCondition The compiledCondition against which records should be matched for select
     * @param apacheIgniteCompiledSelection The compiledSelection against which records should be matched for select
     * @param parameterMap                  Map of stream variable to be used to replace condition
     * @param constantMap                   Map of constants to be used to replace condition
     * @return
     */
    private String getSelectQuery(ApacheIgniteCompiledCondition apacheIgniteCompiledCondition,
                                  ApacheIgniteCompiledSelection apacheIgniteCompiledSelection,
                                  Map<String, Object> parameterMap, Map<Integer, Object> constantMap) {

        String selectors = apacheIgniteCompiledSelection.getCompiledSelectClause().getCompiledQuery();
        String condition = apacheIgniteCompiledCondition.getCompiledQuery();
        StringBuilder selectQuery = new StringBuilder(SELECT).append(WHITESPACE);
        selectQuery.append(selectors).append(WHITESPACE)
                .append(FROM).append(WHITESPACE).append(this.tableName).append(WHITESPACE);
        if (!condition.equals("'*'")) {
            selectQuery.append(WHERE).append(WHITESPACE);
            condition = this.replaceConditionWithParameter(condition, parameterMap, constantMap);
            selectQuery.append(condition);
        }
        ApacheIgniteCompiledCondition compileGroupByClause = apacheIgniteCompiledSelection.getCompiledGroupByClause();
        if (compileGroupByClause != null) {
            selectQuery.append(WHITESPACE).append(GROUP_BY).append(WHITESPACE)
                    .append(compileGroupByClause.getCompiledQuery());
        }
        ApacheIgniteCompiledCondition compileOrderByClause = apacheIgniteCompiledSelection.getCompiledOrderByClause();
        if (compileOrderByClause != null) {
            selectQuery.append(WHITESPACE).append(ORDER_BY).append(WHITESPACE).append(WHITESPACE)
                    .append(compileOrderByClause.getCompiledQuery());
        }
        if (apacheIgniteCompiledSelection.getLimit() != null) {
            selectQuery.append(WHITESPACE).append(LIMIT).append(WHITESPACE)
                    .append(apacheIgniteCompiledSelection.getLimit());
        }
        if (apacheIgniteCompiledSelection.getOffset() != null) {
            selectQuery.append(WHITESPACE).append(OFFSET).append(WHITESPACE)
                    .append(apacheIgniteCompiledSelection.getOffset());
        }
        ApacheIgniteCompiledCondition compileHavingClause = apacheIgniteCompiledSelection.getCompiledHavingClause();
        if (compileHavingClause != null) {
            selectQuery.append(WHITESPACE).append(HAVING).append(WHITESPACE)
                    .append(compileHavingClause.getCompiledQuery());
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

    /**
     * Form a string with attributes to be selected by the query.
     *
     * @param selectAttributeBuilders list of attributes to be selected by the store query.
     * @return return a compiledCondition with attributes to be returned when executing a select query.
     */
    private ApacheIgniteCompiledCondition compileSelectClause(List<SelectAttributeBuilder> selectAttributeBuilders) {

        StringBuilder compiledSelectionList = new StringBuilder();
        SortedMap<Integer, Object> paramMap = new TreeMap<>();
        SortedMap<Integer, Object> paramConstMap = new TreeMap<>();
        int offset = 0;
        for (SelectAttributeBuilder selectAttributeBuilder : selectAttributeBuilders) {
            ApacheIgniteConditionVisitor visitor = new ApacheIgniteConditionVisitor();
            selectAttributeBuilder.getExpressionBuilder().build(visitor);
            String compiledCondition = visitor.returnCondition();
            compiledSelectionList.append(compiledCondition);
            if (selectAttributeBuilder.getRename() != null && !selectAttributeBuilder.getRename().isEmpty()) {
                compiledSelectionList.append(WHITESPACE).append(ApacheIgniteConstants.AS).append(WHITESPACE)
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
            compiledSelectionList.setLength(compiledSelectionList.length() - 1);
        }
        return new ApacheIgniteCompiledCondition(compiledSelectionList.toString(), paramMap, paramConstMap);
    }

    /**
     * Extract names of the attributes to be selected when executing store query
     *
     * @param selectAttributeBuilders list of attributes to be selected by the store query.
     * @return List of attributesNames to be returned when executing a query
     */
    private List<String> getSelectList(List<SelectAttributeBuilder> selectAttributeBuilders) {

        List<String> attributeList = new ArrayList<>();
        selectAttributeBuilders.forEach(selectAttributeBuilder -> {
            ApacheIgniteConditionVisitor visitor = new ApacheIgniteConditionVisitor();
            selectAttributeBuilder.getExpressionBuilder().build(visitor);
            String compiledCondition = visitor.returnCondition();
            attributeList.add(compiledCondition);
        });
        return attributeList;
    }

    /**
     * Compile the matching condition
     *
     * @param expressionBuilders that helps visiting the conditions in order to compile the condition
     * @return compiled condition that used to matching events in having,group by
     */
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

    /**
     * Compile the matching condition for order by
     *
     * @param orderByAttributeBuilders List of attributes used by order y
     * @return that used to matching events in order by
     */
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

    /**
     * Create a table in ignite store if it does not exists already
     *
     * @param primaryKey primary keys of the table to be created.
     */
    private void createTable(Annotation primaryKey) {

        PreparedStatement statement = null;
        try {
            StringBuilder tableCreateQuery = new StringBuilder();
            List<Element> primaryKeyList = (primaryKey == null) ? new ArrayList<>() : primaryKey.getElements();
            tableCreateQuery.append(TABLE_CREATE_QUERY).append(this.tableName).append(OPEN_PARENTHESIS);
            this.attributes.forEach(attribute -> {
                tableCreateQuery.append(attribute.getName()).append(WHITESPACE);
                switch (attribute.getType()) {
                    case BOOL:
                        tableCreateQuery.append(ApacheIgniteConstants.BOOLEAN).append(WHITESPACE);
                        break;
                    case DOUBLE:
                        tableCreateQuery.append(ApacheIgniteConstants.DOUBLE).append(WHITESPACE);
                        break;
                    case FLOAT:
                        tableCreateQuery.append(ApacheIgniteConstants.FLOAT).append(WHITESPACE);
                        break;
                    case INT:
                        tableCreateQuery.append(ApacheIgniteConstants.INTEGER).append(WHITESPACE);
                        break;
                    case LONG:
                        tableCreateQuery.append(ApacheIgniteConstants.LONG).append(WHITESPACE);
                        break;
                    case STRING:
                        tableCreateQuery.append(ApacheIgniteConstants.STRING).append(WHITESPACE);
                        break;
                }
                tableCreateQuery.append(SEPARATOR);
            });
            tableCreateQuery.append(SQL_PRIMARY_KEY_DEF).append(OPEN_PARENTHESIS).
                    append(this.flattenAnnotatedElements(primaryKeyList))
                    .append(CLOSE_PARENTHESIS);
            tableCreateQuery.append(CLOSE_PARENTHESIS);
            tableCreateQuery.append(WHITESPACE).append(WITH).append(WHITESPACE).append(DOUBLE_QUOTES)
                    .append(IGNITE_TEMPLATE).append(EQUAL);
            if (template != null) {
                tableCreateQuery.append(template).append(DOUBLE_QUOTES);
            } else {
                tableCreateQuery.append(IGNITE_PARTITIONED).append(DOUBLE_QUOTES);
            }
            if (backups != null && !backups.isEmpty()) {
                tableCreateQuery.append(SEPARATOR).append(DOUBLE_QUOTES).append(IGNITE_BACKUPS).append(EQUAL)
                        .append(backups).append(DOUBLE_QUOTES);
            }
            if (atomicity != null && !atomicity.isEmpty()) {
                tableCreateQuery.append(SEPARATOR).append(DOUBLE_QUOTES).append(IGNITE_ATOMICITY).append(EQUAL)
                        .append(atomicity).append(DOUBLE_QUOTES);
            }
            if (affinityKey != null && !affinityKey.isEmpty()) {
                tableCreateQuery.append(SEPARATOR).append(DOUBLE_QUOTES).append(IGNITE_AFFINITY_KEY).append(EQUAL)
                        .append(affinityKey).append(DOUBLE_QUOTES);
            }
            if (cacheName != null && !cacheName.isEmpty()) {
                tableCreateQuery.append(SEPARATOR).append(DOUBLE_QUOTES).append(IGNITE_CACHE_NAME).append(EQUAL)
                        .append(cacheName).append(DOUBLE_QUOTES);
            }
            if (dataRegion != null && !dataRegion.isEmpty()) {
                tableCreateQuery.append(SEPARATOR).append(DOUBLE_QUOTES).append(IGNITE_DATA_REGION).append(EQUAL)
                        .append(dataRegion).append(DOUBLE_QUOTES);
            }
            statement = connection.prepareStatement(tableCreateQuery.toString());
            statement.execute();
        } catch (SQLException e) {
            throw new ApacheIgniteTableException("Creating table : " + this.tableName + " failed in ignite store " +
                    e.getMessage(), e);
        } finally {
            ApacheIgniteTableUtils.cleanup(null, statement);
        }
    }

    /**
     * Convert columns of the table in to a comma separated string
     *
     * @return comma separated string of table columns
     */
    private String columnNames() {

        StringBuilder columns = new StringBuilder();
        for (Attribute attribute : attributes) {
            columns.append(attribute.getName()).append(WHITESPACE).append(SEPARATOR);
        }
        columns.delete(columns.length() - 2, columns.length());
        return columns.toString();
    }

    /**
     * @param condition             condition against which records should be matched in find,contains
     * @param conditionParameterMap Map of stream variable to be used to replace condition
     * @param constantMap           constantMap Map of constants to be used to replace condition
     * @return codition with stream variables and constants
     */
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

    /**
     * convert record to comma separated string
     *
     * @param record records to be updated or inserted
     * @return string of records separated by commas
     */
    private String convertAttributesValuesToString(Object[] record) {

        StringBuilder values = new StringBuilder();
        for (Object value : record) {
            if (value instanceof String) {
                values.append("'");
                values.append(value.toString()).append("'").append(SEPARATOR);
            } else {
                values.append(value.toString()).append(WHITESPACE).append(SEPARATOR);
            }
        }
        values.delete(values.length() - 2, values.length());
        return values.toString();
    }

    /**
     * convert list of elements to a comma separated string.
     *
     * @param elements list of element to be converted to string use when creating table
     * @return comma separated string with elements of the string
     */
    private String flattenAnnotatedElements(List<Element> elements) {

        StringBuilder sb = new StringBuilder();
        elements.forEach(elem -> {
            sb.append(elem.getValue());
            if (elements.indexOf(elem) != elements.size() - 1) {
                sb.append(SEPARATOR);
            }
        });
        return sb.toString();
    }
}
