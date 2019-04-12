package org.wso2.extension.siddhi.store.apacheignite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
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
import org.wso2.siddhi.query.api.util.AnnotationHelper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.ANNOTATION_ELEMENT_PASSWORD;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.ANNOTATION_ELEMENT_TABLE_NAME;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.ANNOTATION_ELEMENT_URL;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.ANNOTATION_ELEMENT_USERNAME;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.BOOLEAN;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.CLOSE_PARENTHESIS;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.COLUMNS;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.FLOAT;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.INSERT_QUERY;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.LONG;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.OPEN_PARENTHESIS;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.SEPARATOR;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.SQL_PRIMARY_KEY_DEF;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.STRING;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.TABLE_CREATE_QUERY;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.TABLE_NAME;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.VALUES;
import static org.wso2.extension.siddhi.store.apacheignite.ApacheIgniteConstants.WHITESPACE;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_INDEX;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_PRIMARY_KEY;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_STORE;

/**
 * apacheIgnite store implementation
 */
@Extension(
        name = "apacheignite",
        namespace = "store",
        description = " ",
        parameters = {
               /* @Parameter(
                        name = " ",
                        description = " " ,
                        dynamic = false/true,
                        optional = true/false, defaultValue = " ",
                        type = {DataType.INT, DataType.BOOL, DataType.STRING, DataType.DOUBLE, }
                        ),*/
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
                        syntax = " ",
                        description = " "
                )
        }
)

// for more information refer https://wso2.github.io/siddhi/documentation/siddhi-4.0/#event-table-types

public class ApacheIgniteStore extends AbstractQueryableRecordTable {

    private static final Log log = LogFactory.getLog(ApacheIgniteStore.class);
    private String tableName;
    private String url;
    private String username;
    private String password;
    private String booleanType;
    private String doubleType;
    private String floatType;
    private String integerType;
    private String longType;
    private String stringType;
    private String binaryType;
    private boolean connected;
    private Annotation storeAnnotation;
    private Annotation primaryKey;
    private Annotation indices;
    private List<String> attributeNames;
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
        attributes = tableDefinition.getAttributeList();
        this.attributeNames = tableDefinition.getAttributeList().stream().map(Attribute::getName).
                collect(Collectors.toList());
        this.tableName = ApacheIgniteTableUtils.isEmpty(tableName) ? tableDefinition.getId() : tableName;
        if (ApacheIgniteTableUtils.isEmpty(url)) {
            throw new SiddhiAppCreationException("Required parameter '" + ANNOTATION_ELEMENT_URL + " for DB " +
                    "connectivity  cannot be empty for creating table : " + this.tableName);
        }
        if (primaryKey == null) {
            throw new SiddhiAppCreationException("primary key field cannot be empty for " + this.tableName);
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
            throw new SiddhiAppRuntimeException("insertion failed " + e.getMessage());
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
        log.info(condition);

        PreparedStatement st ;
        ResultSet rs ;
        StringBuilder readQuery = new StringBuilder();
        readQuery.append("SELECT ").append(" * ").append("FROM ").append(this.tableName);
        try {
            if (!condition.equals("'?'")) {
                readQuery.append(" WHERE ");
                for (Map.Entry<String, Object> map : findConditionParameterMap.entrySet()) {

                    Object streamVariable = map.getValue();
                    if (streamVariable instanceof String) {
                        condition = condition.replaceFirst(Pattern.quote("?"), map.getValue().toString());
                    } else {
                        condition = condition.replaceFirst(Pattern.quote("'?'"), map.getValue().toString());
                    }
                }
                readQuery.append(condition);
                log.info(readQuery);
            }
            st = con.prepareStatement(readQuery.toString());
            rs = st.executeQuery();
//            if (rs.toString().isEmpty()) {
//                //ApacheIgniteTableUtils.cleanupConnection(rs, st, con);
//                log.info("No matching records to be retrieved for condition ");
//            } else{
//
//            }
            return new ApacheIgniteIterator(con, st, rs, tableName, attributes);
        } catch (SQLException e) {
            // ApacheIgniteTableUtils.cleanupConnection(rs, st, con); bug
            throw new ApacheIgniteTableException("unable to read " + this.tableName + e.getMessage());
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
        ResultSet rs = null;
        PreparedStatement st = null;
        log.info(condition);
        StringBuilder readQuery = new StringBuilder();
        readQuery.append("SELECT ").append(" * ").append("FROM ").append(this.tableName);
        try {
            if (!condition.equals("'?'")) {
                readQuery.append(" WHERE ");
                for (Map.Entry<String, Object> map : containsConditionParameterMap.entrySet()) {

                    Object streamVariable = map.getValue();
                    if (streamVariable instanceof String) {
                        condition = condition.replaceFirst(Pattern.quote("?"), map.getValue().toString());
                    } else {
                        condition = condition.replaceFirst(Pattern.quote("'?'"), map.getValue().toString());
                    }
                }
                readQuery.append(condition);
                log.info(readQuery);
            }
            st = con.prepareStatement(readQuery.toString());
            rs = st.executeQuery();
            if (rs.next()) {
                ApacheIgniteTableUtils.cleanupConnection(rs, st, con);
                return true;
            } else {
                ApacheIgniteTableUtils.cleanupConnection(rs, st, con);
                log.info("No results matching for given condition ");
                return false;
            }

        } catch (SQLException e) {
            ApacheIgniteTableUtils.cleanupConnection(rs, st, con);
            throw new ApacheIgniteTableException("unable to read " + this.tableName + e.getMessage());
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

        StringBuilder deleteCondition = new StringBuilder();
        PreparedStatement statement = null;
        try {
            deleteCondition.append("DELETE FROM ").append(this.tableName);
            if (!condition.equals("?")) {
                deleteCondition.append(" WHERE ");
                for (Map<String, Object> map : deleteConditionParameterMaps) {

                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        Object streamVariable = entry.getValue();
                        if (streamVariable instanceof String) {
                            condition = condition.replaceFirst(Pattern.quote("?"), entry.getValue().toString());
                        } else {
                            condition = condition.replaceFirst(Pattern.quote("'?'"), entry.getValue().toString());
                        }
                    }
                }
            }
            log.info(condition);
            deleteCondition.append(condition);
            log.info(deleteCondition);
            statement = con.prepareStatement(deleteCondition.toString());
            statement.execute();
            ApacheIgniteTableUtils.cleanupConnection(null, statement, con);
        } catch (SQLException e) {
            ApacheIgniteTableUtils.cleanupConnection(null, statement, con);
            throw new ApacheIgniteTableException("Error deleting recors from the table: " + this.tableName);
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

        log.info("updated*****###");
        ApacheIgniteCompiledCondition igniteCompiledCondition = (ApacheIgniteCompiledCondition) compiledCondition;
        String condition = igniteCompiledCondition.getCompiledQuery();
        PreparedStatement statement = null;
        log.info(condition);

        try {
            for (Map<String, Object> mapCondition : list) {
                for (Map.Entry<String, Object> entry : mapCondition.entrySet()) {
                    Object streamVariable = entry.getValue();
                    if (streamVariable instanceof String) {
                        condition = condition.replaceFirst(Pattern.quote("?"), entry.getValue().toString());
                    } else {
                        condition = condition.replaceFirst(Pattern.quote("'?'"), entry.getValue().toString());
                    }
                }
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
            throw new ApacheIgniteTableException("error updating records " + this.tableName);
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

        log.info("updated*************");
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
            // statement.close();
            ApacheIgniteTableUtils.cleanupConnection(null, statement, con);
            //do throw exception if key other than primary key defined with on condition.
            //check with two primary keys.
        } catch (SQLException e) {
            throw new SiddhiAppRuntimeException("insertion or updating failed " + e.getMessage());
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

        ApacheIgniteConditionVisitor visitor = new ApacheIgniteConditionVisitor(this.tableName);
        expressionBuilder.build(visitor);
        return new ApacheIgniteCompiledCondition(visitor.returnCondition(), visitor.getParameters(),
                visitor.isContainsConditionExist(), visitor.getOrdinalOfContainPattern());
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
            con = DriverManager.getConnection(url);
            this.createTable(storeAnnotation, primaryKey, indices);
            connected = true;
        } catch (SQLException e) {
            throw new ConnectionUnavailableException("unable to connect " + e.getMessage(), e);
        } catch (ClassNotFoundException e) {
            throw new ConnectionUnavailableException("unable to find class" + e.getMessage(), e);
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

        try {
            if (connected) {
                con.close();
            }
        } catch (SQLException e) {
            throw new ApacheIgniteTableException("unable to close connection");
        }
    }

    @Override
    protected RecordIterator<Object[]> query(Map<String, Object> map, CompiledCondition compiledCondition,
                                             CompiledSelection compiledSelection, Attribute[] attributes)
            throws ConnectionUnavailableException {

        return null;
    }

    @Override
    protected CompiledSelection compileSelection(List<SelectAttributeBuilder> list, List<ExpressionBuilder> list1,
                                                 ExpressionBuilder expressionBuilder,
                                                 List<OrderByAttributeBuilder> list2,
                                                 Long aLong, Long aLong1) {

        return null;
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
                    case STRING://check in rdbms
                        tableCreateQuery.append(STRING);
                        break;
                }
                tableCreateQuery.append(SEPARATOR);
            });
            tableCreateQuery.append(SQL_PRIMARY_KEY_DEF).append(OPEN_PARENTHESIS).
                    append(this.flattenAnnotatedElements(primaryKeyList))
                    .append(CLOSE_PARENTHESIS);
            tableCreateQuery.append(CLOSE_PARENTHESIS);
            sts = con.prepareStatement(tableCreateQuery.toString());
            sts.execute();
            ApacheIgniteTableUtils.cleanupConnection(null, sts, con);
            log.info(tableCreateQuery);
        } catch (SQLException e) {
            ApacheIgniteTableUtils.cleanupConnection(null, sts, con);
            throw new ApacheIgniteTableException("table ceation failed " + e.getMessage(), e);
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

