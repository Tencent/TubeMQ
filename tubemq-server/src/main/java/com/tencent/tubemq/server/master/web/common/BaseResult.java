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

import com.tencent.tubemq.corebase.utils.TStringUtils;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.builder.ToStringBuilder;


/**
 * Paging algorithm package.
 *   * <p/>
 *   * Pagination must be set: TotalItem (the total number of bars), the default is 0,
 * should be set in dao PageSize (number of pages per page), should be set in the web
 * layer QueryBase defaults to 20, subclasses can be overwritten getDefaultPageSize()
 * Modify CurrentPage (current page), default is 1, home page, should be set in the
 * web layer. After paging, you can get: TotalPage (total number of pages) FristItem
 * (the current page starts recording position, counting from 1) PageLastItem
 * (current page last recording position) On the page, the number of pages displayed
 * per page should be: lines , the current page name should be: page
 *   * <p/>
 *   * Add the render link function at the same time,
 * the subclass overrides the getParameteres method and returns valid parameters.
 */
public class BaseResult implements Serializable {
    private static final long serialVersionUID = 8807356835558347735L;
    private static final Integer defaultPageSize = new Integer(20);
    private static final Integer defaultFristPage = new Integer(1);
    private static final Integer defaultTotleItem = new Integer(0);
    /**
     * max page size
     */
    private static final int MAX_PAGE_SIZE = 501;
    private Integer totalItem;
    private Integer pageSize;
    private Integer currentPage;
    // for paging
    private int startRow;
    private int endRow;
    private Map tempParam;
    private Object removeObject;
    // for ajax
    private String ajaxPrefix;
    private String ajaxSuffix;
    private String charset;
    private String from;
    private boolean escape = true;
    private boolean jsEscape = false;

    /**
     * parse date
     */
    public static final Date parseDate(String dateTime, String format, Date def) {
        Date date = def;
        try {
            DateFormat formatter = new SimpleDateFormat(format);
            date = formatter.parse(dateTime);
        } catch (Exception e) {
            DateFormat f = DateFormat.getDateInstance();
            try {
                date = f.parse(dateTime);
            } catch (Exception ee) {
                // ignore
            }
        }
        return date;
    }

    /**
     * Encoding according to the escape method in javascript
     *
     * @return Encoded string
     */
    public static final String jsEncode(String str) {
        if (null == str) {
            return null;
        }

        char[] cs = str.toCharArray();
        StringBuffer sBuilder = new StringBuffer();

        for (int i = 0; i < cs.length; ++i) {
            int c = cs[i] & 0xFFFF;

            if (((c >= '0') && (c <= '9')) || (((c >= 'a') && (c <= 'z')) || ((c >= 'A') && (c <= 'Z')))) {
                sBuilder.append(cs[i]);
            } else {
                sBuilder.append('%');

                if (c > 255) {
                    sBuilder.append('u');
                }

                sBuilder.append(Integer.toHexString(c));
            }
        }

        return sBuilder.toString();
    }

    /**
     * Parsing a string encoded with javascript's escape,
     * equivalent to javascript's unescape
     *
     * @return Encoded string
     */
    public static final String jsDecode(String str) {
        if (null == str) {
            return null;
        }

        StringBuffer sBuilder = new StringBuffer();
        char[] cs = str.toCharArray();

        for (int i = 0; i < cs.length; ++i) {
            int c = cs[i] & 0xFFFF;

            if (c == '%') {
                if (cs[i + 1] == 'u') {
                    if ((i + 6) > cs.length) {
                        sBuilder.append(cs[i]);
                    } else {
                        try {
                            char cc = (char) Integer.parseInt(new String(cs, i + 2, 4), 16);
                            sBuilder.append(cc);
                            i += 5;
                        } catch (Exception e) {
                            sBuilder.append(cs[i]);
                        }
                    }
                } else {
                    if ((i + 3) > cs.length) {
                        sBuilder.append(cs[i]);
                    } else {
                        try {
                            char cc = (char) Integer.parseInt(new String(cs, i + 1, 2), 16);
                            sBuilder.append(cc);
                            i += 2;
                        } catch (Exception e) {
                            sBuilder.append(cs[i]);
                        }
                    }
                }
            } else {
                sBuilder.append(cs[i]);
            }
        }

        return sBuilder.toString();
    }

    /**
     * @return Returns the defaultPageSize.
     */
    protected Integer getDefaultPageSize() {
        return defaultPageSize;
    }

    public boolean isFirstPage() {
        return this.getCurrentPage().intValue() == 1;
    }

    public int getPreviousPage() {
        int back = this.getCurrentPage().intValue() - 1;

        if (back <= 0) {
            back = 1;
        }

        return back;
    }

    public boolean isLastPage() {
        return this.getTotalPage() == this.getCurrentPage().intValue();
    }

    public int getNextPage() {
        int back = this.getCurrentPage().intValue() + 1;

        if (back > this.getTotalPage()) {
            back = this.getTotalPage();
        }

        return back;
    }

    /**
     * @return Returns the currentPage.
     */
    public Integer getCurrentPage() {
        if (currentPage == null) {
            return defaultFristPage;
        }

        return currentPage;
    }

    /**
     * @param cPage The currentPage to set.
     */
    public void setCurrentPage(Integer cPage) {
        if ((cPage == null) || (cPage.intValue() <= 0)) {
            this.currentPage = null;
        } else {
            this.currentPage = cPage;
        }
        setStartEndRow();
    }

    public void setCurrentPageString(String pageString) {
        if (isBlankString(pageString)) {
            this.setCurrentPage(defaultFristPage);
        }

        try {
            Integer integer = new Integer(pageString);
            this.setCurrentPage(integer);
        } catch (NumberFormatException ignore) {
            this.setCurrentPage(defaultFristPage);
        }
    }

    private void setStartEndRow() {
        this.startRow = this.getPageSize().intValue() * (this.getCurrentPage().intValue() - 1);
        this.endRow = this.startRow + this.getPageSize().intValue();
    }

    /**
     * @return Returns the pageSize.
     */
    public Integer getPageSize() {
        if (pageSize == null) {
            return getDefaultPageSize();
        }

        return pageSize;
    }

    /**
     * @param pSize The pageSize to set.
     */
    public void setPageSize(Integer pSize) {

        if ((pSize == null) || (pSize.intValue() < 0)) {
            this.pageSize = null;
        } else if (pSize > MAX_PAGE_SIZE || pSize < 1) {
            throw new IllegalArgumentException("每页显示条数范围为1~" + MAX_PAGE_SIZE + "条");
        } else {
            this.pageSize = pSize;
        }
        setStartEndRow();
    }

    public boolean hasSetPageSize() {
        return pageSize != null;
    }

    public void setPageSizeString(String pageSizeString) {
        if (isBlankString(pageSizeString)) {
            return;
        }

        try {
            Integer integer = new Integer(pageSizeString);
            if (integer > MAX_PAGE_SIZE || integer < 1) {
                throw new IllegalArgumentException("每页显示条数范围为1~" + MAX_PAGE_SIZE + "条");
            }
            this.setPageSize(integer);
        } catch (NumberFormatException ignore) {
            //
        }
    }

    /**
     * @param pageSizeString
     * @return
     */
    private boolean isBlankString(String pageSizeString) {
        if (pageSizeString == null) {
            return true;
        }

        if (pageSizeString.trim().length() == 0) {
            return true;
        }

        return false;
    }

    /**
     * @return Returns the totalItem.
     */
    public Integer getTotalItem() {
        if (totalItem == null) {
            // throw new IllegalStateException("Please set the TotalItem
            // frist.");
            return defaultTotleItem;
        }

        return totalItem;
    }

    /**
     * @param tItem The totalItem to set.
     */
    public void setTotalItem(Integer tItem) {
        if (tItem == null) {
            throw new IllegalArgumentException("TotalItem can't be null.");
        }

        this.totalItem = tItem;

        int current = this.getCurrentPage().intValue();
        int lastPage = this.getTotalPage();

        if (current > lastPage) {
            this.setCurrentPage(new Integer(lastPage));
        }
    }

    public int getTotalPage() {
        int pgSize = this.getPageSize().intValue();
        int total = this.getTotalItem().intValue();
        int result = total / pgSize;

        if ((total == 0) || ((total % pgSize) != 0)) {
            result++;
        }

        return result;
    }

    /**
     * Frist是first写错了 历史原因，不改了
     */
    public int getPageFristItem() {
        int cPage = this.getCurrentPage().intValue();

        if (cPage == 1) {
            return 1; // 第一页开始当然是第 1 条记录
        }

        cPage--;

        int pgSize = this.getPageSize().intValue();

        return (pgSize * cPage) + 1;
    }

    public int getPageLastItem() {
        int cPage = this.getCurrentPage().intValue();
        int pgSize = this.getPageSize().intValue();
        int assumeLast = pgSize * cPage;
        int totalItem = getTotalItem().intValue();

        if (assumeLast > totalItem) {
            return totalItem;
        } else {
            return assumeLast;
        }
    }

    /**
     * @return Returns the endRow.
     */
    public int getEndRow() {
        return endRow;
    }

    /**
     * @param endRow The endRow to set.
     */
    public void setEndRow(int endRow) {
        this.endRow = endRow;
    }

    /**
     * @return Returns the startRow.
     */
    public int getStartRow() {
        return startRow;
    }

    /**
     * @param startRow The startRow to set.
     */
    public void setStartRow(int startRow) {
        this.startRow = startRow;
    }

    protected String getSQLBlurValue(String value) {
        if (value == null) {
            return null;
        }

        return value + '%';
    }

    protected String formatDate(String datestring) {
        if (TStringUtils.isBlank(datestring)) {
            return null;
        } else {
            return (datestring + " 00:00:00");
        }
    }

    /**
     * When the time is queried, the end time is 23:59:59
     */
    protected String addDateEndPostfix(String datestring) {
        if (TStringUtils.isBlank(datestring)) {
            return null;
        }

        return datestring + " 23:59:59";
    }

    /**
     * When the time is queried, the start time is 00:00:00
     */
    protected String addDateStartPostfix(String datestring) {
        if (TStringUtils.isBlank(datestring)) {
            return null;
        }

        return datestring + " 00:00:00";
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    public String[] getParameteres() {
        return new String[0];
    }

    public List getTaobaoSlider() {
        List l = new ArrayList(10);
        int leftStart = 1;
        int leftEnd = 2;
        int mStart = this.getCurrentPage().intValue() - 2;
        int mEnd = this.getCurrentPage().intValue() + 2;
        int rStart = this.getTotalPage() - 1;
        int rEnd = this.getTotalPage();
        if (mStart <= leftEnd) {
            leftStart = 0;
            leftEnd = 0;
            mStart = 1;
        }
        if (mEnd >= rStart) {
            rStart = 0;
            rEnd = 0;
            mEnd = this.getTotalPage();
        }
        if (leftEnd > leftStart) {
            for (int i = leftStart; i <= leftEnd; ++i) {
                l.add(String.valueOf(i));
            }
            l.add("...");
        }
        for (int i = mStart; i <= mEnd; ++i) {
            l.add(String.valueOf(i));
        }
        if (rEnd > rStart) {
            l.add("...");
            for (int i = rStart; i <= rEnd; ++i) {
                l.add(String.valueOf(i));
            }
        }
        return l;
    }

    /**
     * @return Returns the ajaxPrefix.
     */
    public String getAjaxPrefix() {
        return ajaxPrefix;
    }

    /**
     * @param ajaxPrefix The ajaxPrefix to set.
     */
    public BaseResult setAjaxPrefix(String ajaxPrefix) {
        this.ajaxPrefix = ajaxPrefix;
        return this;
    }

    /**
     * @return Returns the ajaxSuffix.
     */
    public String getAjaxSuffix() {
        return ajaxSuffix;
    }

    /**
     * @param ajaxSuffix The ajaxSuffix to set.
     */
    public BaseResult setAjaxSuffix(String ajaxSuffix) {
        this.ajaxSuffix = ajaxSuffix;
        return this;
    }

    /**
     * @return Returns the charset.
     */
    public String getCharset() {
        return charset;
    }

    /**
     * @param charset The charset to set.
     */
    public BaseResult setCharset(String charset) {
        this.charset = charset;
        return this;
    }

    /**
     * Remove a parameter
     */
    public BaseResult remove(Object key) {
        if (null == this.removeObject) {
            this.removeObject = new Object();
        }
        replace(key, this.removeObject);
        return this;
    }

    /**
     * Temporarily modify the value of a parameter
     */
    public BaseResult replace(Object key, Object val) {
        if (null != key && null != val) {
            if (null == this.tempParam) {
                this.tempParam = new HashMap(5);
            }
            this.tempParam.put(String.valueOf(key), val);
        }
        return this;
    }

    /**
     * @return Returns the from.
     */
    public String getFrom() {
        return from;
    }

    /**
     * @param from The from to set.
     */
    public void setFrom(String from) {
        this.from = from;
    }

    /**
     * @return Returns the escape.
     */
    public boolean isEscape() {
        return escape;
    }

    /**
     * @param escape The escape to set.
     */
    public BaseResult setEscape(boolean escape) {
        this.escape = escape;
        return this;
    }

    /**
     * @return Returns the jsEscape.
     */
    public final boolean isJsEscape() {
        return this.jsEscape;
    }

    /**
     * @param jsEscape The jsEscape to set.
     */
    public final BaseResult setJsEscape(boolean jsEscape) {
        this.jsEscape = jsEscape;
        return this;
    }
}
