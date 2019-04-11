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

/**
 * Implementation of class corresponding to Apache Ignite store
 */
public class ApacheIgniteCompiledSelection implements CompiledSelection {

//    private ApacheIgniteCompiledCondition compiledSelectClause;
//    private ApacheIgniteCompiledCondition compiledGroupByClause;
//    private ApacheIgniteCompiledCondition compiledHavingClause;
//    private ApacheIgniteCompiledCondition compiledOrderByClause;
//    private Long limit;
//    private Long offset;
//
//    public ApacheIgniteCompiledSelection(ApacheIgniteCompiledCondition compiledSelectClause,
//                                         ApacheIgniteCompiledCondition compiledGroupByClause,
//                                         ApacheIgniteCompiledCondition compiledHavingClause,
//                                         ApacheIgniteCompiledCondition compiledOrderByClause,
//                                         Long limit, Long offset) {
//
//        this.compiledSelectClause = compiledSelectClause;
//        this.compiledGroupByClause = compiledGroupByClause;
//        this.compiledHavingClause = compiledHavingClause;
//        this.compiledOrderByClause = compiledOrderByClause;
//        this.limit = limit;
//        this.offset = offset;
//    }

    @Override
    public CompiledSelection cloneCompilation(String s) {

        return null;
    }
}
