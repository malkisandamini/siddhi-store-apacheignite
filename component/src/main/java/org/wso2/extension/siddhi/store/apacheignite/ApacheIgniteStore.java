package org.wso2.extension.siddhi.store.apacheignite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.table.record.AbstractQueryableRecordTable;
//import org.wso2.siddhi.core.table.record.AbstractRecordTable;
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

        try {
            StringBuilder insertQuery = new StringBuilder();
            for (Object[] record : records) {
                insertQuery.append("INSERT INTO ").append(tableName)
                        .append(" (").append(this.columnNames()).append(")").append(" values ")
                        .append("(").append(this.convertAttributesValuesToString(record)).append(")");
            }
            log.info(insertQuery);
            PreparedStatement statement = con.prepareStatement(insertQuery.toString());
            statement.execute();
            statement.close();
        } catch (SQLException e) {
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
            PreparedStatement st = con.prepareStatement(readQuery.toString());
            ResultSet rs = st.executeQuery();
            if (rs.toString().isEmpty()) {
                log.info("No matching records to be retrieved for condition ");
            }
//            rs.close();
//            st.close();
            return new ApacheIgniteIterator(con, st, rs, tableName, attributes);
        } catch (SQLException e) {
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
            PreparedStatement st = con.prepareStatement(readQuery.toString());
            ResultSet rs = st.executeQuery();
            if (rs.next()) {
                return true;
            } else {
                log.info("No results matching for given condition ");
                return false;
            }
        } catch (SQLException e) {
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
        PreparedStatement statement;
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
            statement.close();
        } catch (SQLException e) {
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
            //try{
            StringBuilder updateCondition = new StringBuilder();
            updateCondition.append(" UPDATE ").append(tableName).append(WHITESPACE)
                    .append("SET").append(WHITESPACE).append(this.mapAttributesWithValues(list1)).append("WHERE")
                    .append(WHITESPACE).append(condition);

            PreparedStatement statement = con.prepareStatement(updateCondition.toString());
            statement.execute();

            //}
            log.info(updateCondition);
        } catch (SQLException e) {
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
            statement.close();
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

        StringBuilder tableCreateQuery = new StringBuilder();
        List<Element> primaryKeyList = (primaryKey == null) ? new ArrayList<>() : primaryKey.getElements();
        tableCreateQuery.append("CREATE TABLE IF NOT EXISTS ").append(this.tableName).append(" ( ");
        this.attributes.forEach(attribute -> {
            tableCreateQuery.append(attribute.getName()).append(WHITESPACE);
            switch (attribute.getType()) {
                case BOOL:
                    tableCreateQuery.append("boolean ");
                    break;
                case DOUBLE:
                    tableCreateQuery.append("double ");
                    break;
                case FLOAT:
                    tableCreateQuery.append("float ");
                    break;
                case INT:
                    tableCreateQuery.append("integer ");
                    break;
                case LONG:
                    tableCreateQuery.append("long ");
                    break;
//                case OBJECT:
//                    tableCreateQuery.append(binaryType);
//                    break;
                case STRING://check in rdbms
                    tableCreateQuery.append("varchar");
                    break;
            }

            tableCreateQuery.append(", ");
        });
        tableCreateQuery.append("PRIMARY KEY ( ").append(this.flattenAnnotatedElements(primaryKeyList)).append(")");
        //tableCreateQuery.delete(tableCreateQuery.length()-2,tableCreateQuery.length());
        tableCreateQuery.append(" ) ");
        PreparedStatement sts = con.prepareStatement(tableCreateQuery.toString());
        sts.execute();

        log.info(tableCreateQuery);
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
