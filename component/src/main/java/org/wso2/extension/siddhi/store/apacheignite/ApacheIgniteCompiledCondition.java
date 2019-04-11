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

import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;

import java.util.SortedMap;

/**
 * Implementation of class corresponding to Apache Ignite store
 */

public class ApacheIgniteCompiledCondition implements CompiledCondition {

    private String compiledQuery;
    private SortedMap<Integer, Object> parameters;
    private boolean isContainsConditionExists;
    private int ordinalOfContainPattern;

    public ApacheIgniteCompiledCondition(String compiledQuery, SortedMap<Integer, Object> parameters,
                                         boolean isContainsConditionExists, int
                                                 ordinalOfContainPattern) {

        this.compiledQuery = compiledQuery;
        this.parameters = parameters;
        this.isContainsConditionExists = isContainsConditionExists;
        this.ordinalOfContainPattern = ordinalOfContainPattern;
    }

    @Override
    public CompiledCondition cloneCompilation(String s) {

        return new ApacheIgniteCompiledCondition(this.compiledQuery, this.parameters, this.isContainsConditionExists,
                this.ordinalOfContainPattern);
    }

    public String getCompiledQuery() {

        return compiledQuery;
    }

    public boolean isContainsConditionExists() {

        return isContainsConditionExists;
    }

    public String toString() {

        return getCompiledQuery();
    }

    public SortedMap<Integer, Object> getParameters() {

        return parameters;
    }

    public int getOrdinalOfContainPattern() {

        return ordinalOfContainPattern;
    }
}