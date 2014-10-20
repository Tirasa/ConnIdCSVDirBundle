/* 
 * ====================
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2011 ConnId. All rights reserved.
 * 
 * The contents of this file are subject to the terms of the Common Development
 * and Distribution License("CDDL") (the "License").  You may not use this file
 * except in compliance with the License.
 * 
 * You can obtain a copy of the License at
 * http://opensource.org/licenses/cddl1.php
 * See the License for the specific language governing permissions and limitations
 * under the License.
 * 
 * When distributing the Covered Code, include this CDDL Header Notice in each file
 * and include the License file at http://opensource.org/licenses/cddl1.php.
 * If applicable, add the following below this CDDL Header, with the fields
 * enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 * ====================
 */
package net.tirasa.connid.bundles.csvdir;

import java.nio.charset.Charset;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.framework.spi.ConfigurationProperty;

/**
 * Configuration information required for the Connector to attach to a file.
 */
public class CSVDirConfiguration extends AbstractConfiguration {

    private String keyseparator = ",";

    private String multivalueSeparator;

    /**
     * Regular expression describing files to be processed
     */
    private String fileMask;

    /**
     * Absolute path of a directory where the CSV files to be processed are located
     */
    private String sourcePath;

    /**
     * Name of the column used to specify users to be deleted
     */
    private String deleteColumnName;

    /**
     * Basic encoding of the file the default valid in the default character set of the OS.
     */
    private String encoding = Charset.defaultCharset().name();

    /**
     * Delimiter to determine beginning and end of text in value.
     */
    private char textQualifier = '"';

    /**
     * Delimiter used to separate fields in CSV files.
     */
    private char fieldDelimiter = ',';

    /**
     * Name of the column used to identify user uniquely.
     */
    private String[] keyColumnNames;

    /**
     * Name of the column used to specify user password.
     */
    private String passwordColumnName;

    /**
     * Specify if value quotation is required.
     */
    private Boolean quotationRequired = Boolean.TRUE;

    /**
     * Column names separated by comma.
     */
    private String[] fields;

    /**
     * Specify it first line file must be ignored.
     */
    private Boolean ignoreHeader = Boolean.TRUE;

    /*
     * Status column name.
     */
    private String statusColumn;

    /**
     * Value for 'statusColumn' field to indicate disabled entries. Default 'true'.
     */
    private String enabledStatusValue = "true";

    /**
     * Value for 'statusColumn' field to indicate disabled entries. Default 'false'.
     */
    private String disabledStatusValue = "false";

    /**
     * Default value for 'statusColumn' field. Default 'true'.
     */
    private String defaultStatusValue = "true";

    @ConfigurationProperty(displayMessageKey = "sourcePath.display",
            helpMessageKey = "sourcePath.help", required = true, order = 1)
    public String getSourcePath() {
        return sourcePath;
    }

    @ConfigurationProperty(displayMessageKey = "fileMask.display",
            helpMessageKey = "fileMask.help", required = true, order = 2)
    public final String getFileMask() {
        return fileMask;
    }

    @ConfigurationProperty(displayMessageKey = "encoding.display",
            helpMessageKey = "encoding.help", order = 3)
    public final String getEncoding() {
        return encoding;
    }

    @ConfigurationProperty(displayMessageKey = "fieldDelimiter.display",
            helpMessageKey = "fieldDelimiter.help", order = 4)
    public char getFieldDelimiter() {
        return fieldDelimiter;
    }

    /**
     * from http://hsqldb.org/doc/2.0/guide/texttables-chapt.html#ttc_configuration
     *
     * @return field delimiter expression to be used with HSQLDB
     */
    public String getEscapedFieldDelimiter() {
        final String escaped;
        switch (fieldDelimiter) {

            case ';':
                escaped = "\\semi";
                break;

            case '\'':
                escaped = "\\quote";
                break;

            case ' ':
                escaped = "\\space";
                break;

            case '`':
                escaped = "\\apos";
                break;

            default:
                escaped = String.valueOf(fieldDelimiter);
        }
        return escaped;
    }

    @ConfigurationProperty(displayMessageKey = "textQualifier.display",
            helpMessageKey = "textQualifier.help", order = 5)
    public char getTextQualifier() {
        return textQualifier;
    }

    @ConfigurationProperty(displayMessageKey = "keyColumnName.display",
            helpMessageKey = "keyColumnName.help", required = true, order = 6)
    public String[] getKeyColumnNames() {
        return keyColumnNames == null ? null : keyColumnNames.clone();
    }

    @ConfigurationProperty(displayMessageKey = "passwordColumnName.display",
            helpMessageKey = "passwordColumnName.help", order = 7)
    public String getPasswordColumnName() {
        return passwordColumnName;
    }

    @ConfigurationProperty(displayMessageKey = "deleteColumnName.display",
            helpMessageKey = "deleteColumnName.help", order = 8)
    public String getDeleteColumnName() {
        return deleteColumnName;
    }

    @ConfigurationProperty(displayMessageKey = "quotationRequired.display",
            helpMessageKey = "quotationRequired.help", order = 9)
    public Boolean getQuotationRequired() {
        return quotationRequired;
    }

    @ConfigurationProperty(displayMessageKey = "fields.display",
            helpMessageKey = "fields.help", required = true, order = 10)
    public String[] getFields() {
        return fields == null ? null : fields.clone();
    }

    @ConfigurationProperty(displayMessageKey = "ignoreHeader.display",
            helpMessageKey = "ignoreHeader.help", order = 11)
    public Boolean getIgnoreHeader() {
        return ignoreHeader;
    }

    @ConfigurationProperty(displayMessageKey = "keyseparator.display",
            helpMessageKey = "keyseparator.help", order = 12)
    public String getKeyseparator() {
        return keyseparator;
    }

    @ConfigurationProperty(displayMessageKey = "multivalueSeparator.display",
            helpMessageKey = "multivalueSeparator.help", order = 13)
    public String getMultivalueSeparator() {
        return multivalueSeparator;
    }

    @ConfigurationProperty(displayMessageKey = "defaultStatusValue.display",
            helpMessageKey = "defaultStatusValue.help", required = false, order = 14)
    public String getDefaultStatusValue() {
        return defaultStatusValue;
    }

    @ConfigurationProperty(displayMessageKey = "disabledStatusValue.display",
            helpMessageKey = "disabledStatusValue.help", required = false, order = 15)
    public String getDisabledStatusValue() {
        return disabledStatusValue;
    }

    @ConfigurationProperty(displayMessageKey = "enabledStatusValue.display",
            helpMessageKey = "enabledStatusValue.help", required = false, order = 16)
    public String getEnabledStatusValue() {
        return enabledStatusValue;
    }

    @ConfigurationProperty(displayMessageKey = "statusColumn.display",
            helpMessageKey = "statusColumn.help", required = false, order = 17)
    public String getStatusColumn() {
        return statusColumn;
    }

    public void setKeyseparator(final String keyseparator) {
        this.keyseparator = keyseparator;
    }

    public void setMultivalueSeparator(final String multivalueSeparator) {
        this.multivalueSeparator = multivalueSeparator;
    }

    public void setIgnoreHeader(final Boolean ignoreHeader) {
        if (ignoreHeader != null) {
            this.ignoreHeader = ignoreHeader;
        }
    }

    public void setDeleteColumnName(final String deleteColumnName) {
        this.deleteColumnName = deleteColumnName;
    }

    public void setEncoding(final String encoding) {
        if (encoding == null) {
            this.encoding = Charset.defaultCharset().name();
        } else {
            this.encoding = encoding;
        }
    }

    public void setFieldDelimiter(final char fieldDelimeter) {
        this.fieldDelimiter = fieldDelimeter;
    }

    public void setFileMask(final String fileMask) {
        this.fileMask = fileMask;
    }

    public void setKeyColumnNames(final String[] keyColumnNames) {
        this.keyColumnNames = keyColumnNames == null ? null : keyColumnNames.clone();
    }

    public void setSourcePath(final String sourcePath) {
        this.sourcePath = sourcePath;
    }

    public void setTextQualifier(final char textQualifier) {
        this.textQualifier = textQualifier;
    }

    public void setPasswordColumnName(final String passwordColumnName) {
        this.passwordColumnName = passwordColumnName;
    }

    public void setFields(final String[] fields) {
        this.fields = fields == null ? null : fields.clone();
    }

    public void setQuotationRequired(final Boolean quotationRequired) {
        if (quotationRequired != null) {
            this.quotationRequired = quotationRequired;
        }
    }

    public void setDefaultStatusValue(final String defaultStatusValue) {
        this.defaultStatusValue = defaultStatusValue;
    }

    public void setDisabledStatusValue(final String disabledStatusValue) {
        this.disabledStatusValue = disabledStatusValue;
    }

    public void setEnabledStatusValue(final String enabledStatusValue) {
        this.enabledStatusValue = enabledStatusValue;
    }

    public void setStatusColumn(final String statusColumn) {
        this.statusColumn = statusColumn;
    }

    /**
     * Determine if all the values are valid.
     *
     * @throws IllegalArgumentException if encoding or fileMask or sourcePath or keyColumnName or passwordColumnName or
     * deleteColumnName or fields is blank or null.
     * @throws IllegalStateException if the text qualifier and field delimiter are the same.
     * @throws RuntimeException if the file is not found.
     * @throws java.nio.charset.IllegalCharsetNameException if the character set name is invalid
     * @see org.identityconnectors.framework.Configuration#validate()
     */
    @Override
    public void validate() {
        // make sure the encoding is set..
        if (this.encoding == null) {
            throw new IllegalArgumentException("File encoding must not be null!");
        }

        //make sure it's a valid charset
        Charset.forName(this.encoding);

        // make sure the delimiter and the text qualifier are not the same..
        if (this.textQualifier == this.fieldDelimiter) {
            throw new IllegalStateException("Field delimiter and text qualifier can not be equal!");
        }

        // make sure file mask is set..
        if (StringUtil.isBlank(this.fileMask)) {
            throw new IllegalArgumentException("File mask must not be blank!");
        }

        // make sure source path is set..
        if (StringUtil.isBlank(this.sourcePath)) {
            throw new IllegalArgumentException("Source path must not be blank!");
        }

        // make sure keyColumnName is set..
        if (this.keyColumnNames == null || this.keyColumnNames.length == 0) {
            throw new IllegalArgumentException("key column name must not be blank!");
        }
        // make sure fields is set..
        if (this.fields == null || this.fields.length == 0) {
            throw new IllegalArgumentException("Column names must not be blank!");
        }
        // make sure key separator is set..
        if (StringUtil.isBlank(this.keyseparator)) {
            throw new IllegalArgumentException("File mask must not be blank!");
        }
    }
}
