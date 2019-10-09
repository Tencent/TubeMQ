/*
 * Tencent is pleased to support the open source community by making TubeMQ available.
 *
 * Copyright (C) 2012-2019 Tencent. All Rights Reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.tubemq.corebase.utils;

import com.tencent.tubemq.corebase.TBaseConstants;
import com.tencent.tubemq.corebase.TokenConstants;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.HmacUtils;

/**
 * Utility to String operations.
 * Modified version of <a href="https://github.com/webx/citrus">citrus Project</a>
 */
public class TStringUtils {

    public static final String EMPTY = "";


    /**
     * <p>Checks if a String is empty ("") or null.</p>
     * <p>
     * <pre>
     * TStringUtils.isEmpty(null)      = true
     * TStringUtils.isEmpty("")        = true
     * TStringUtils.isEmpty(" ")       = false
     * TStringUtils.isEmpty("bob")     = false
     * TStringUtils.isEmpty("  bob  ") = false
     * </pre>
     * <p>
     * <p>NOTE: This method changed in Lang version 2.0. It no longer trims the String. That
     * functionality is available in isBlank().</p>
     *
     * @param str the String to check, may be null
     * @return <code>true</code> if the String is empty or null
     */
    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }

    /**
     * <p>Checks if a String is not empty ("") and not null.</p>
     * <p>
     * <pre>
     * TStringUtils.isNotEmpty(null)      = false
     * TStringUtils.isNotEmpty("")        = false
     * TStringUtils.isNotEmpty(" ")       = true
     * TStringUtils.isNotEmpty("bob")     = true
     * TStringUtils.isNotEmpty("  bob  ") = true
     * </pre>
     *
     * @param str the String to check, may be null
     * @return <code>true</code> if the String is not empty and not null
     */
    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }

    /**
     * <p>Checks if a String is whitespace, empty ("") or null.</p>
     * <p>
     * <pre>
     * TStringUtils.isBlank(null)      = true
     * TStringUtils.isBlank("")        = true
     * TStringUtils.isBlank(" ")       = true
     * TStringUtils.isBlank("bob")     = false
     * TStringUtils.isBlank("  bob  ") = false
     * </pre>
     *
     * @param str the String to check, may be null
     * @return <code>true</code> if the String is null, empty or whitespace
     * @since 2.0
     */
    public static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if ((!Character.isWhitespace(str.charAt(i)))) {
                return false;
            }
        }
        return true;
    }

    /**
     * <p>Checks if a String is not empty (""), not null and not whitespace only.</p>
     * <p>
     * <pre>
     * TStringUtils.isNotBlank(null)      = false
     * TStringUtils.isNotBlank("")        = false
     * TStringUtils.isNotBlank(" ")       = false
     * TStringUtils.isNotBlank("bob")     = true
     * TStringUtils.isNotBlank("  bob  ") = true
     * </pre>
     *
     * @param str the String to check, may be null
     * @return <code>true</code> if the String is not empty and not null and not whitespace
     * @since 2.0
     */
    public static boolean isNotBlank(String str) {
        return !isBlank(str);
    }

    public static String trim(String str) {
        return str == null ? null : str.trim();
    }

    public static String trimToNull(String str) {
        String ts = trim(str);
        return isEmpty(ts) ? null : ts;
    }

    public static String trimToEmpty(String str) {
        return str == null ? EMPTY : str.trim();
    }

    public static boolean equals(String str1, String str2) {
        return str1 == null ? str2 == null : str1.equals(str2);
    }

    public static boolean equalsIgnoreCase(String str1, String str2) {
        return str1 == null ? str2 == null : str1.equalsIgnoreCase(str2);
    }

    // ==========================================================================
    // Case conversion.
    // ==========================================================================

    /**
     * Convert the first character of a string to uppercase
     * (<code>Character.toTitleCase</code>） <p> if string is
     * <code>null</code>return<code>null</code>。
     * <p/>
     * <pre>
     * TStringUtils.capitalize(null)  = null
     * TStringUtils.capitalize("")    = ""
     * TStringUtils.capitalize("cat") = "Cat"
     * TStringUtils.capitalize("cAt") = "CAt"
     * </pre>
     * <p/>
     * </p>
     *
     * @param str The string to be converted
     * @return Convert the first character of a string to upper case
     * if it is <code>null</code>，return<code>null</code>
     */
    public static String capitalize(String str) {
        int strLen;

        if (str == null || (strLen = str.length()) == 0) {
            return str;
        }

        return new StringBuilder(strLen)
                .append(Character.toTitleCase(str.charAt(0)))
                .append(str.substring(1))
                .toString();
    }

    /**
     * <p/>
     * <pre>
     * TStringUtils.uncapitalize(null)  = null
     * TStringUtils.uncapitalize("")    = ""
     * TStringUtils.uncapitalize("Cat") = "cat"
     * TStringUtils.uncapitalize("CAT") = "CAT"
     * </pre>
     * <p/>
     * </p>
     *
     * @param str The string to be converted
     * @return Convert the first character of a string to lower case
     * if it is <code>null</code>，return<code>null</code>
     */
    public static String uncapitalize(String str) {
        int strLen;

        if (str == null || (strLen = str.length()) == 0) {
            return str;
        }

        if (strLen > 1
                && Character.isUpperCase(str.charAt(1))
                && Character.isUpperCase(str.charAt(0))) {
            return str;
        }

        return new StringBuilder(strLen)
                .append(Character.toLowerCase(str.charAt(0)))
                .append(str.substring(1))
                .toString();
    }

    /**
     * <p/>
     * <pre>
     * TStringUtils.swapCase(null)                 = null
     * TStringUtils.swapCase("")                   = ""
     * TStringUtils.swapCase("The dog has a BONE") = "tHE DOG HAS A bone"
     * </pre>
     * <p/>
     * </p>
     *
     * @param str The string to be converted
     * @return Case inverted character string
     * if it is <code>null</code>，return<code>null</code>
     */
    public static String swapCase(String str) {
        int strLen;

        if (str == null || (strLen = str.length()) == 0) {
            return str;
        }

        StringBuilder buffer = new StringBuilder(strLen);

        char ch = 0;

        for (int i = 0; i < strLen; i++) {
            ch = str.charAt(i);

            if (Character.isUpperCase(ch)) {
                ch = Character.toLowerCase(ch);
            } else if (Character.isTitleCase(ch)) {
                ch = Character.toLowerCase(ch);
            } else if (Character.isLowerCase(ch)) {
                ch = Character.toUpperCase(ch);
            }

            buffer.append(ch);
        }

        return buffer.toString();
    }

    /**
     * <p/>
     * <pre>
     * TStringUtils.toUpperCase(null)  = null
     * TStringUtils.toUpperCase("")    = ""
     * TStringUtils.toUpperCase("aBc") = "ABC"
     * </pre>
     * <p/>
     * </p>
     *
     * @param str The string to be converted
     * @return Convert the string to upper case
     * if it is <code>null</code>，return<code>null</code>
     */
    public static String toUpperCase(String str) {
        if (str == null) {
            return null;
        }

        return str.toUpperCase();
    }

    /**
     * <p/>
     * <pre>
     * TStringUtils.toLowerCase(null)  = null
     * TStringUtils.toLowerCase("")    = ""
     * TStringUtils.toLowerCase("aBc") = "abc"
     * </pre>
     * <p/>
     * </p>
     *
     * @param str The string to be converted
     * @return Convert the string to lower case
     * if it is <code>null</code>，return<code>null</code>
     */
    public static String toLowerCase(String str) {
        if (str == null) {
            return null;
        }

        return str.toLowerCase();
    }

    /**
     * <p/>
     * <pre>
     * TStringUtils.toCamelCase(null)  = null
     * TStringUtils.toCamelCase("")    = ""
     * TStringUtils.toCamelCase("aBc") = "aBc"
     * TStringUtils.toCamelCase("aBc def") = "aBcDef"
     * TStringUtils.toCamelCase("aBc def_ghi") = "aBcDefGhi"
     * TStringUtils.toCamelCase("aBc def_ghi 123") = "aBcDefGhi123"
     * </pre>
     * <p/>
     * </p> <p> This method preserves all separators except underscores and whitespace. </p>
     *
     * @param str The string to be converted
     * @return Convert the string to Camel Case
     * if it is <code>null</code>，return<code>null</code>
     */
    public static String toCamelCase(String str) {
        return new MyWordTokenizer(1).parse(str);
    }

    /**
     * <p/>
     * <pre>
     * TStringUtils.toPascalCase(null)  = null
     * TStringUtils.toPascalCase("")    = ""
     * TStringUtils.toPascalCase("aBc") = "ABc"
     * TStringUtils.toPascalCase("aBc def") = "ABcDef"
     * TStringUtils.toPascalCase("aBc def_ghi") = "ABcDefGhi"
     * TStringUtils.toPascalCase("aBc def_ghi 123") = "aBcDefGhi123"
     * </pre>
     * <p/>
     * </p> <p> This method preserves all separators except underscores and whitespace. </p>
     *
     * @param str The string to be converted
     * @return Convert the string to Pascal Case
     * if it is <code>null</code>，return<code>null</code>
     */
    public static String toPascalCase(String str) {
        return new MyWordTokenizer(3).parse(str);
    }

    /**
     * <p/>
     * <pre>
     * TStringUtils.toUpperCaseWithUnderscores(null)  = null
     * TStringUtils.toUpperCaseWithUnderscores("")    = ""
     * TStringUtils.toUpperCaseWithUnderscores("aBc") = "A_BC"
     * TStringUtils.toUpperCaseWithUnderscores("aBc def") = "A_BC_DEF"
     * TStringUtils.toUpperCaseWithUnderscores("aBc def_ghi") = "A_BC_DEF_GHI"
     * TStringUtils.toUpperCaseWithUnderscores("aBc def_ghi 123") = "A_BC_DEF_GHI_123"
     * TStringUtils.toUpperCaseWithUnderscores("__a__Bc__") = "__A__BC__"
     * </pre>
     * <p/>
     * </p> <p> This method preserves all separators except whitespace. </p>
     *
     * @param str The string to be converted
     * @return Convert the string to Upper Case With Underscores
     * if it is <code>null</code>，return<code>null</code>
     */
    public static String toUpperCaseWithUnderscores(String str) {
        return new MyWordTokenizer(4).parse(str);
    }

    /**
     * <p/>
     * <pre>
     * TStringUtils.toLowerCaseWithUnderscores(null)  = null
     * TStringUtils.toLowerCaseWithUnderscores("")    = ""
     * TStringUtils.toLowerCaseWithUnderscores("aBc") = "a_bc"
     * TStringUtils.toLowerCaseWithUnderscores("aBc def") = "a_bc_def"
     * TStringUtils.toLowerCaseWithUnderscores("aBc def_ghi") = "a_bc_def_ghi"
     * TStringUtils.toLowerCaseWithUnderscores("aBc def_ghi 123") = "a_bc_def_ghi_123"
     * TStringUtils.toLowerCaseWithUnderscores("__a__Bc__") = "__a__bc__"
     * </pre>
     * <p/>
     * </p> <p> This method preserves all separators except whitespace. </p>
     *
     * @param str The string to be converted
     * @return Convert the string to lower Case With Underscores
     * if it is <code>null</code>，return<code>null</code>
     */
    public static String toLowerCaseWithUnderscores(String str) {
        return new MyWordTokenizer(2).parse(str);
    }

    // Misc
    //-----------------------------------------------------------------------

    /**
     * <p>Find the Levenshtein distance between two Strings.</p>
     * <p>
     * <p>This is the number of changes needed to change one String into another, where each change
     * is a single character modification (deletion, insertion or substitution).</p>
     * <p>
     * <p>The previous implementation of the Levenshtein distance algorithm was from <a
     * href="http://www.merriampark.com/ld.htm">http://www.merriampark.com/ld.htm</a></p>
     * <p>
     * <p>Chas Emerick has written an implementation in Java, which avoids an OutOfMemoryError which
     * can occur when my Java implementation is used with very large strings.<br> This
     * implementation of the Levenshtein distance algorithm is from <a
     * href="http://www.merriampark.com/ldjava.htm">http://www.merriampark.com/ldjava.htm</a></p>
     * <p>
     * <pre>
     * TStringUtils.getLevenshteinDistance(null, *)             = IllegalArgumentException
     * TStringUtils.getLevenshteinDistance(*, null)             = IllegalArgumentException
     * TStringUtils.getLevenshteinDistance("","")               = 0
     * TStringUtils.getLevenshteinDistance("","a")              = 1
     * TStringUtils.getLevenshteinDistance("aaapppp", "")       = 7
     * TStringUtils.getLevenshteinDistance("frog", "fog")       = 1
     * TStringUtils.getLevenshteinDistance("fly", "ant")        = 3
     * TStringUtils.getLevenshteinDistance("elephant", "hippo") = 7
     * TStringUtils.getLevenshteinDistance("hippo", "elephant") = 7
     * TStringUtils.getLevenshteinDistance("hippo", "zzzzzzzz") = 8
     * TStringUtils.getLevenshteinDistance("hello", "hallo")    = 1
     * </pre>
     *
     * @param s the first String, must not be null
     * @param t the second String, must not be null
     * @return result distance
     * @throws IllegalArgumentException if either String input <code>null</code>
     */
    public static int getLevenshteinDistance(String s, String t) {
        if (s == null || t == null) {
            throw new IllegalArgumentException("Strings must not be null");
        }

        /*
           The difference between this impl. and the previous is that, rather
           than creating and retaining a matrix of size s.length()+1 by t.length()+1,
           we maintain two single-dimensional arrays of length s.length()+1.  The first, d,
           is the 'current working' distance array that maintains the newest distance cost
           counts as we iterate through the characters of String s.  Each time we increment
           the index of String t we are comparing, d is copied to p, the second int[].  Doing so
           allows us to retain the previous cost counts as required by the algorithm (taking
           the minimum of the cost count to the left, up one, and diagonally up and to the left
           of the current cost count being calculated).  (Note that the arrays aren't really
           copied anymore, just switched...this is clearly much better than cloning an array
           or doing a System.arraycopy() each time  through the outer loop.)

           Effectively, the difference between the two implementations is this one does not
           cause an out of memory condition when calculating the LD over two very large strings.
         */

        int n = s.length(); // length of s
        int m = t.length(); // length of t

        if (n == 0) {
            return m;
        } else if (m == 0) {
            return n;
        }

        if (n > m) {
            // swap the input strings to consume less memory
            String tmp = s;
            s = t;
            t = tmp;
            n = m;
            m = t.length();
        }

        int[] p = new int[n + 1]; //'previous' cost array, horizontally
        int[] d = new int[n + 1]; // cost array, horizontally
        int[] swap; // swap helper to assist in swapping p and d

        // indexes into strings s and t
        int i; // iterates through s
        int j; // iterates through t

        int cost; // cost

        for (i = 0; i <= n; i++) {
            p[i] = i;
        }

        char ch; // jth character of t
        for (j = 1; j <= m; j++) {
            ch = t.charAt(j - 1);
            d[0] = j;

            for (i = 1; i <= n; i++) {
                cost = s.charAt(i - 1) == ch ? 0 : 1;
                // minimum of cell to the left+1, to the top+1, diagonally left and up +cost
                d[i] = Math.min(Math.min(d[i - 1] + 1, p[i] + 1), p[i - 1] + cost);
            }

            // copy current distance counts to 'previous row' distance counts
            swap = p;
            p = d;
            d = swap;
        }

        // our last action in the above loop was to switch d and p, so p now
        // actually has the most recent cost counts
        return p[n];
    }

    public static String arrayToString(String[] strs) {
        if (strs.length == 0) {
            return "";
        }
        StringBuffer sbuf = new StringBuffer();
        sbuf.append(strs[0]);
        for (int idx = 1; idx < strs.length; idx++) {
            sbuf.append(",");
            sbuf.append(strs[idx]);
        }
        return sbuf.toString();
    }

    public static String stringifyException(Throwable e) {
        StringWriter stm = new StringWriter();
        PrintWriter wrt = new PrintWriter(stm);
        e.printStackTrace(wrt);
        wrt.close();
        return stm.toString();
    }

    public static String getAuthSignature(final String usrName,
                                          final String usrPassWord,
                                          long timestamp, int randomValue) {
        Base64 base64 = new Base64();
        StringBuilder sbuf = new StringBuilder(512);
        byte[] baseStr =
                base64.encode(HmacUtils.hmacSha1(usrPassWord,
                        sbuf.append(usrName).append(timestamp).append(randomValue).toString()));
        sbuf.delete(0, sbuf.length());
        String signature = "";
        try {
            signature = URLEncoder.encode(new String(baseStr,
                            TBaseConstants.META_DEFAULT_CHARSET_NAME),
                    TBaseConstants.META_DEFAULT_CHARSET_NAME);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return signature;
    }

    public static String setAttrValToAttributes(String srcAttrs,
                                                String attrKey, String attrVal) {
        StringBuilder sbuf = new StringBuilder(512);
        if (isBlank(srcAttrs)) {
            return sbuf.append(attrKey).append(TokenConstants.EQ).append(attrVal).toString();
        }
        if (!srcAttrs.contains(attrKey)) {
            return sbuf.append(srcAttrs)
                    .append(TokenConstants.SEGMENT_SEP)
                    .append(attrKey).append(TokenConstants.EQ).append(attrVal).toString();
        }
        boolean notFirst = false;
        String[] strAttrs = srcAttrs.split(TokenConstants.SEGMENT_SEP);
        for (String strAttrItem : strAttrs) {
            if (isNotBlank(strAttrItem)) {
                if (notFirst) {
                    sbuf.append(TokenConstants.SEGMENT_SEP);
                }
                if (strAttrItem.contains(attrKey)) {
                    sbuf.append(attrKey).append(TokenConstants.EQ).append(attrVal);
                } else {
                    sbuf.append(strAttrItem);
                }
                notFirst = true;
            }
        }
        return sbuf.toString();
    }

    public static String getAttrValFrmAttributes(String srcAttrs, String attrKey) {
        if (!isBlank(srcAttrs)) {
            String[] strAttrs = srcAttrs.split(TokenConstants.SEGMENT_SEP);
            for (String attrItem : strAttrs) {
                if (isNotBlank(attrItem)) {
                    String[] kv = attrItem.split(TokenConstants.EQ);
                    if (attrKey.equals(kv[0])) {
                        return kv[1];
                    }
                }
            }
        }
        return null;
    }

    /**
     * 解析出下列语法所构成的<code>SENTENCE</code>。
     * <p/>
     * <pre>
     *  SENTENCE = WORD (DELIMITER* WORD)*
     *
     *  WORD = UPPER_CASE_WORD | LOWER_CASE_WORD | TITLE_CASE_WORD | DIGIT_WORD
     *
     *  UPPER_CASE_WORD = UPPER_CASE_LETTER+
     *  LOWER_CASE_WORD = LOWER_CASE_LETTER+
     *  TITLE_CASE_WORD = UPPER_CASE_LETTER LOWER_CASE_LETTER+
     *  DIGIT_WORD      = DIGIT+
     *
     *  UPPER_CASE_LETTER = Character.isUpperCase()
     *  LOWER_CASE_LETTER = Character.isLowerCase()
     *  DIGIT             = Character.isDigit()
     *  NON_LETTER_DIGIT  = !Character.isUpperCase() && !Character.isLowerCase() &&
     * !Character.isDigit()
     *
     *  DELIMITER = WHITESPACE | NON_LETTER_DIGIT
     * </pre>
     */

    // A tokenizer that performs string processing on a string according to a specified type
    private static class MyWordTokenizer extends WordTokenizer {
        // Participle type, wordTokenType range of values [-2,1,2,,3,4]
        // -2,1 : toCamelCase
        //    2 : toLowerCaseWithUnderscores
        //    3 : toPascalCase
        //    4 : toUpperCaseWithUnderscores
        int wordTokenType = TBaseConstants.META_VALUE_UNDEFINED;

        public MyWordTokenizer(int wordTokenType) {
            this.wordTokenType = wordTokenType;
        }

        @Override
        protected void startSentence(StringBuilder buffer, char ch) {
            switch (wordTokenType) {
                case 4: {
                    buffer.append(Character.toUpperCase(ch));
                    break;
                }

                case 3: {
                    buffer.append(Character.toUpperCase(ch));
                    break;
                }

                case 2: {
                    buffer.append(Character.toLowerCase(ch));
                    break;
                }

                case 1:
                default: {
                    buffer.append(Character.toLowerCase(ch));
                    break;
                }
            }
        }

        @Override
        protected void startWord(StringBuilder buffer, char ch) {
            switch (wordTokenType) {
                case 4: {
                    if (!isDelimiter(buffer.charAt(buffer.length() - 1))) {
                        buffer.append(UNDERSCORE);
                    }
                    buffer.append(Character.toUpperCase(ch));
                    break;
                }

                case 3: {
                    buffer.append(Character.toUpperCase(ch));
                    break;
                }

                case 2: {
                    if (!isDelimiter(buffer.charAt(buffer.length() - 1))) {
                        buffer.append(UNDERSCORE);
                    }
                    buffer.append(Character.toLowerCase(ch));
                    break;
                }

                case 1:
                default: {
                    if (!isDelimiter(buffer.charAt(buffer.length() - 1))) {
                        buffer.append(Character.toUpperCase(ch));
                    } else {
                        buffer.append(Character.toLowerCase(ch));
                    }
                    break;
                }
            }
        }

        @Override
        protected void inWord(StringBuilder buffer, char ch) {
            switch (wordTokenType) {
                case 4: {
                    buffer.append(Character.toUpperCase(ch));
                    break;
                }

                case 3: {
                    buffer.append(Character.toLowerCase(ch));
                    break;
                }

                case 2: {
                    buffer.append(Character.toLowerCase(ch));
                    break;
                }

                case 1:
                default: {
                    buffer.append(Character.toLowerCase(ch));
                    break;
                }
            }
        }

        @Override
        protected void startDigitSentence(StringBuilder buffer, char ch) {
            switch (wordTokenType) {
                case 4: {
                    buffer.append(ch);
                    break;
                }

                case 3: {
                    buffer.append(ch);
                    break;
                }

                case 2: {
                    buffer.append(ch);
                    break;
                }

                case 1:
                default: {
                    buffer.append(ch);
                    break;
                }
            }
        }

        @Override
        protected void startDigitWord(StringBuilder buffer, char ch) {
            switch (wordTokenType) {
                case 4: {
                    if (!isDelimiter(buffer.charAt(buffer.length() - 1))) {
                        buffer.append(UNDERSCORE);
                    }
                    buffer.append(ch);
                    break;
                }

                case 3: {
                    buffer.append(ch);
                    break;
                }

                case 2: {
                    if (!isDelimiter(buffer.charAt(buffer.length() - 1))) {
                        buffer.append(UNDERSCORE);
                    }
                    buffer.append(ch);
                    break;
                }

                case 1:
                default: {
                    buffer.append(ch);
                    break;
                }
            }
        }

        @Override
        protected void inDigitWord(StringBuilder buffer, char ch) {
            switch (wordTokenType) {
                case 4: {
                    buffer.append(ch);
                    break;
                }

                case 3: {
                    buffer.append(ch);
                    break;
                }

                case 2: {
                    buffer.append(ch);
                    break;
                }

                case 1:
                default: {
                    buffer.append(ch);
                    break;
                }
            }
        }

        @Override
        protected void inDelimiter(StringBuilder buffer, char ch) {
            switch (wordTokenType) {
                case 4: {
                    buffer.append(ch);
                    break;
                }

                case 3: {
                    if (ch != UNDERSCORE) {
                        buffer.append(ch);
                    }
                    break;
                }

                case 2: {
                    buffer.append(ch);
                    break;
                }

                case 1:
                default: {
                    if (ch != UNDERSCORE) {
                        buffer.append(ch);
                    }
                    break;
                }
            }
        }
    }


    private abstract static class WordTokenizer {
        protected static final char UNDERSCORE = '_';

        /**
         * Parse sentence。
         */
        public String parse(String str) {
            if (isEmpty(str)) {
                return str;
            }

            int length = str.length();
            StringBuilder buffer = new StringBuilder(length);

            for (int index = 0; index < length; index++) {
                char ch = str.charAt(index);

                // 忽略空白。
                if (Character.isWhitespace(ch)) {
                    continue;
                }

                // 大写字母开始：UpperCaseWord或是TitleCaseWord。
                if (Character.isUpperCase(ch)) {
                    int wordIndex = index + 1;

                    while (wordIndex < length) {
                        char wordChar = str.charAt(wordIndex);

                        if (Character.isUpperCase(wordChar)) {
                            wordIndex++;
                        } else if (Character.isLowerCase(wordChar)) {
                            wordIndex--;
                            break;
                        } else {
                            break;
                        }
                    }

                    // 1. wordIndex == length, indicating that the last letter is uppercase
                    //      and is handled by upperCaseWord.
                    // 2. wordIndex == index, indicating that index is a titleCaseWord.
                    // 3. wordIndex > index, indicating that index to wordIndex - 1 is all uppercase,
                    //      treated with upperCaseWord.
                    if (wordIndex == length || wordIndex > index) {
                        index = parseUpperCaseWord(buffer, str, index, wordIndex);
                    } else {
                        index = parseTitleCaseWord(buffer, str, index);
                    }

                    continue;
                }

                // start with LowerCaseWord。
                if (Character.isLowerCase(ch)) {
                    index = parseLowerCaseWord(buffer, str, index);
                    continue;
                }

                // start with DigitWord。
                if (Character.isDigit(ch)) {
                    index = parseDigitWord(buffer, str, index);
                    continue;
                }

                // start with Delimiter。
                inDelimiter(buffer, ch);
            }

            return buffer.toString();
        }

        private int parseUpperCaseWord(StringBuilder buffer, String str, int index, int length) {
            char ch = str.charAt(index++);

            // The first letter must exist and be capitalized.
            if (buffer.length() == 0) {
                startSentence(buffer, ch);
            } else {
                startWord(buffer, ch);
            }

            //Subsequent letters must be lowercase.
            for (; index < length; index++) {
                ch = str.charAt(index);
                inWord(buffer, ch);
            }

            return index - 1;
        }

        private int parseLowerCaseWord(StringBuilder buffer, String str, int index) {
            char ch = str.charAt(index++);

            // The first letter must exist and be lower case.
            if (buffer.length() == 0) {
                startSentence(buffer, ch);
            } else {
                startWord(buffer, ch);
            }

            // Subsequent letters must be lowercase.
            int length = str.length();

            for (; index < length; index++) {
                ch = str.charAt(index);

                if (Character.isLowerCase(ch)) {
                    inWord(buffer, ch);
                } else {
                    break;
                }
            }

            return index - 1;
        }

        private int parseTitleCaseWord(StringBuilder buffer, String str, int index) {
            char ch = str.charAt(index++);

            // The first letter must exist and be capitalized.
            if (buffer.length() == 0) {
                startSentence(buffer, ch);
            } else {
                startWord(buffer, ch);
            }

            // Subsequent letters must be lowercase.
            int length = str.length();

            for (; index < length; index++) {
                ch = str.charAt(index);

                if (Character.isLowerCase(ch)) {
                    inWord(buffer, ch);
                } else {
                    break;
                }
            }

            return index - 1;
        }

        private int parseDigitWord(StringBuilder buffer, String str, int index) {
            char ch = str.charAt(index++);

            // The first character, must exist and be a number.
            if (buffer.length() == 0) {
                startDigitSentence(buffer, ch);
            } else {
                startDigitWord(buffer, ch);
            }

            // Subsequent characters must be numbers.
            int length = str.length();

            for (; index < length; index++) {
                ch = str.charAt(index);

                if (Character.isDigit(ch)) {
                    inDigitWord(buffer, ch);
                } else {
                    break;
                }
            }

            return index - 1;
        }

        protected boolean isDelimiter(char ch) {
            return ((!Character.isUpperCase(ch))
                    && (!Character.isLowerCase(ch))
                    && (!Character.isDigit(ch)));
        }

        protected abstract void startSentence(StringBuilder buffer, char ch);

        protected abstract void startWord(StringBuilder buffer, char ch);

        protected abstract void inWord(StringBuilder buffer, char ch);

        protected abstract void startDigitSentence(StringBuilder buffer, char ch);

        protected abstract void startDigitWord(StringBuilder buffer, char ch);

        protected abstract void inDigitWord(StringBuilder buffer, char ch);

        protected abstract void inDelimiter(StringBuilder buffer, char ch);
    }
}
