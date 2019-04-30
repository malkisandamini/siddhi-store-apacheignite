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

import org.wso2.siddhi.core.util.collection.operator.CompiledSelection;

import java.util.List;

/**
 * Implementation of class corresponding to Apache Ignite store.
 * Maintains the compiled select,group by, having, order by, limit clauses.
 */
public class ApacheIgniteCompiledSelection implements CompiledSelection {

    private ApacheIgniteCompiledCondition compiledSelectClause;
    private ApacheIgniteCompiledCondition compiledGroupByClause;
    private ApacheIgniteCompiledCondition compiledHavingClause;
    private ApacheIgniteCompiledCondition compiledOrderByClause;
    private Long limit;
    private Long offset;
    private List<String> selectAttributes;

    public ApacheIgniteCompiledSelection(
            ApacheIgniteCompiledCondition compiledSelectClause,
            ApacheIgniteCompiledCondition compiledGroupByClause,
            ApacheIgniteCompiledCondition compiledHavingClause,
            ApacheIgniteCompiledCondition compiledOrderByClause,
            Long limit, Long offset, List<String> selectAttributes) {

        this.compiledSelectClause = compiledSelectClause;
        this.compiledGroupByClause = compiledGroupByClause;
        this.compiledHavingClause = compiledHavingClause;
        this.compiledOrderByClause = compiledOrderByClause;
        this.limit = limit;
        this.offset = offset;
        this.selectAttributes = selectAttributes;
    }

    @Override
    public CompiledSelection cloneCompilation(String s) {

        return new ApacheIgniteCompiledSelection(compiledSelectClause, compiledGroupByClause,
                compiledHavingClause, compiledOrderByClause, limit, offset, selectAttributes);
    }

    public ApacheIgniteCompiledCondition getCompiledSelectClause() {

        return compiledSelectClause;
    }

    public ApacheIgniteCompiledCondition getCompiledGroupByClause() {

        return compiledGroupByClause;

    }

    public ApacheIgniteCompiledCondition getCompiledHavingClause() {

        return compiledHavingClause;
    }

    public ApacheIgniteCompiledCondition getCompiledOrderByClause() {

        return compiledOrderByClause;
    }

    public Long getLimit() {

        return limit;
    }

    public Long getOffset() {

        return offset;
    }

    public List<String> getSelectedAttributes() {

        return selectAttributes;
    }

}
