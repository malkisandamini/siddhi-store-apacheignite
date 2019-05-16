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

/**
 * Class which holds constants required by Apache Ignite store implementation.
 */
public class ApacheIgniteConstants {

    public static final String ANNOTATION_ELEMENT_URL = "url";
    public static final String ANNOTATION_ELEMENT_TABLE_NAME = "table.name";
    public static final String ANNOTATION_ELEMENT_AUTH_ENABLED = "auth.enabled";
    public static final String ANNOTATION_ELEMENT_USERNAME = "username";
    public static final String ANNOTATION_ELEMENT_PASSWORD = "password";
    public static final String SCHEMA = "schema";
    public static final String TEMPLATE = "template";
    public static final String DISTRIBUTE_JOINS = "distributed.joins";
    public static final String ENFORCE_JOIN_ORDER = "enforce.join.order";
    public static final String COLLOCATED = "collocated";
    public static final String REPLICATED_ONLY = "replicated.only";
    public static final String AUTO_CLOSE_SERVER_CURSER = "auto.close.server.cursor";
    public static final String SOCKET_SEND_BUFFER = "socket.send.buffer";
    public static final String SOCKET_RECEIVE_BUFFER = "socket.receive.buffer";
    public static final String BACKUPS = "backups";
    public static final String ATOMICITY = "atomicity";
    public static final String AFFINITY_KEY = "affinity.key";
    public static final String CACHE_NAME = "cache.name";
    public static final String DATA_REGION = "data.region";
    public static final String IGNITE_USER = "user";
    public static final String IGNITE_PASSWORD = "password";
    public static final String IGNITE_TEMPLATE = "template";
    public static final String IGNITE_DISTRIBUTE_JOINS = "distributeJoins";
    public static final String IGNITE_ENFORCE_JOIN_ORDER = "enforceJoinOrder";
    public static final String IGNITE_COLLOCATED = "collocated";
    public static final String IGNITE_REPLICATED_ONLY = "replicatedOnly";
    public static final String IGNITE_AUTO_CLOSE_SERVER_CURSER = "autocloseServerCursor";
    public static final String IGNITE_SOCKET_SEND_BUFFER = "socketSendBuffer";
    public static final String IGNITE_SOCKET_RECEIVE_BUFFER = "socketReceiveBuffer";

    public static final String IGNITE_BACKUPS = "Backups";
    public static final String IGNITE_ATOMICITY = "Atomicity";
    public static final String IGNITE_AFFINITY_KEY = "affinity_Key";
    public static final String IGNITE_CACHE_NAME = "Cache_name";
    public static final String IGNITE_DATA_REGION = "Data_region";
    public static final String IGNITE_PARTITIONED = "PARTITIONED";

    public static final String SQL_PRIMARY_KEY_DEF = "PRIMARY KEY";
    public static final String SQL_AND = "AND";
    public static final String SQL_OR = "OR";
    public static final String SQL_NOT = "NOT";
    public static final String SQL_COMPARE_EQUAL = "=";
    public static final String SQL_COMPARE_GREATER_THAN = ">";
    public static final String SQL_COMPARE_GREATER_THAN_EQUAL = ">=";
    public static final String SQL_COMPARE_LESS_THAN = "<";
    public static final String SQL_COMPARE_LESS_THAN_EQUAL = "<=";
    public static final String SQL_COMPARE_NOT_EQUAL = "<>";
    public static final String SQL_IS_NULL = "NULL";
    public static final String SQL_IN = "IN";

    public static final String SQL_MATH_ADD = "+";
    public static final String SQL_MATH_DIVIDE = "/";
    public static final String SQL_MATH_MOD = "%";
    public static final String SQL_MATH_MULTIPLY = "*";
    public static final String SQL_MATH_SUBTRACT = "-";

    public static final String OPEN_PARENTHESIS = "(";
    public static final String CLOSE_PARENTHESIS = ")";
    public static final String WHITESPACE = " ";
    public static final String SEPARATOR = ",";
    public static final String SEMICOLON = ";";
    public static final String EQUAL = "=";
    public static final String SLASH = "/";
    public static final String DOUBLE_QUOTES = "\"";

    public static final String BOOLEAN = "boolean";
    public static final String DOUBLE = "double";
    public static final String STRING = "varchar";
    public static final String LONG = "long";
    public static final String FLOAT = "float";
    public static final String INTEGER = "int";

    public static final String INSERT_QUERY = "insert into {{tableName}} ({{columns}}) values ({{values}})";
    public static final String COLUMNS = "{{columns}}";
    public static final String VALUES = "{{values}}";
    public static final String TABLE_NAME = "{{tableName}}";
    public static final String TABLE_CREATE_QUERY = "CREATE TABLE IF NOT EXISTS  ";
    public static final String SELECT = "SELECT";
    public static final String FROM = "FROM";
    public static final String WHERE = "WHERE";
    public static final String DELETE = "DELETE";
    public static final String UPDATE = "UPDATE";
    public static final String SET = "SET";
    public static final String MERGE = "MERGE INTO";
    public static final String VALUE = "VALUES";
    public static final String AS = "AS";
    public static final String WITH = "WITH";
    public static final String GROUP_BY = "group by";
    public static final String ORDER_BY = "order by";
    public static final String LIMIT = "limit";
    public static final String OFFSET = "offset";
    public static final String HAVING = "having";


}
