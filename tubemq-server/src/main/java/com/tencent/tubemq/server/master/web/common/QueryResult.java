/*
 * Tencent is pleased to support the open source community by making TubeMQ available.
 *
 * Copyright (C) 2012-2019 Tencent. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.tubemq.server.master.web.common;

import java.util.List;


public class QueryResult<E> {

    private List<E> resultList;

    private int currentPage;

    private int lastPage;

    private int nextPage;

    private int totalPage;

    private int totalCount;

    private int pageSize;

    public List<E> getResultList() {
        return resultList;
    }

    public void setResultList(List<E> resultList) {
        this.resultList = resultList;
    }

    public int getCurrentPage() {
        return currentPage;
    }

    public void setCurrentPage(int currentPage) {
        if (currentPage <= 1) {
            this.currentPage = this.lastPage = 1;
        } else {
            this.currentPage = currentPage;
            this.lastPage = currentPage - 1;
        }

        if (currentPage >= totalPage) {
            this.nextPage = totalPage;
        } else {
            this.nextPage = currentPage + 1;
        }
    }

    public int getLastPage() {
        return lastPage;
    }

    public int getNextPage() {
        return nextPage;
    }

    public int getTotalPage() {
        return totalPage;
    }

    public void setTotalPage(int totalPage) {
        if (totalPage <= 1) {
            this.currentPage = this.lastPage = this.nextPage = 1;
        } else {
            this.totalPage = totalPage;
        }
        if (this.currentPage > totalPage) {
            this.currentPage = totalPage;
        }
        this.nextPage = this.currentPage + 1;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }
}
