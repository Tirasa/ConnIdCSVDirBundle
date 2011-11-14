/*
 * ====================
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 2011 Tirasa. All rights reserved.
 *
 * The contents of this file are subject to the terms of the Common Development
 * and Distribution License("CDDL") (the "License").  You may not use this file
 * except in compliance with the License.
 *
 * You can obtain a copy of the License at
 * http://IdentityConnectors.dev.java.net/legal/license.txt
 * See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * When distributing the Covered Code, include this
 * CDDL Header Notice in each file
 * and include the License file at identityconnectors/legal/license.txt.
 * If applicable, add the following below this CDDL Header, with the fields
 * enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 * ====================
 */
package org.connid.csvdir;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Set;

public class CSVDirConnectorTestsSharedMethods {

    public CSVDirConnectorTestsSharedMethods() {
    }

    protected CSVDirConfiguration createConfiguration(
            final String mask) {
        // create the connector configuration..
        final CSVDirConfiguration config = new CSVDirConfiguration();
        config.setFileMask(mask);
        config.setKeyColumnNames(new String[]{
                    TestAccountsValue.ACCOUNTID, TestAccountsValue.FIRSTNAME});
        config.setDeleteColumnName(TestAccountsValue.DELETED);
        config.setPasswordColumnName(TestAccountsValue.PASSWORD);
        config.setSourcePath(System.getProperty("java.io.tmpdir"));
        config.setQuotationRequired(Boolean.TRUE);
        config.setIgnoreHeader(Boolean.TRUE);
        config.setKeyseparator(";");
        config.setFields(new String[]{
                    TestAccountsValue.ACCOUNTID,
                    TestAccountsValue.FIRSTNAME,
                    TestAccountsValue.LASTNAME,
                    TestAccountsValue.EMAIL,
                    TestAccountsValue.CHANGE_NUMBER,
                    TestAccountsValue.PASSWORD,
                    TestAccountsValue.DELETED});
        config.validate();
        return config;
    }

    protected File createFile(
            final String name, final Set<TestAccount> testAccounts)
            throws IOException {
        final File file = File.createTempFile(name, ".csv");
        file.deleteOnExit();

        final PrintWriter wrt = writeOutFileData(file);
        writeOutEachUser(wrt, testAccounts);
        wrt.close();
        return file;
    }

    private void writeOutEachUser(final PrintWriter wrt,
            final Set<TestAccount> testAccounts) {
        wrt.println(TestAccountsValue.HEADER.toLine(
                TestAccountsValue.FIELD_DELIMITER,
                TestAccountsValue.TEXT_QUALIFIER));
        for (TestAccount user : testAccounts) {
            wrt.println(user.toLine(TestAccountsValue.FIELD_DELIMITER,
                    TestAccountsValue.TEXT_QUALIFIER));
        }
    }

    protected File createSampleFile(String name, int THOUSANDS)
            throws IOException {
        TestAccount account;

        final File file = File.createTempFile(name, ".csv");
        file.deleteOnExit();

        final PrintWriter wrt = writeOutFileData(file);

        wrt.println(TestAccountsValue.HEADER.toLine(
                TestAccountsValue.FIELD_DELIMITER,
                TestAccountsValue.TEXT_QUALIFIER));

        for (int i = 0; i < THOUSANDS; i++) {
            account = new TestAccount(
                    "accountid" + i,
                    "firstname",
                    "lastname",
                    "email",
                    "changeNumber",
                    "password",
                    "no");

            wrt.println(account.toLine(
                    TestAccountsValue.FIELD_DELIMITER,
                    TestAccountsValue.TEXT_QUALIFIER));
        }
        wrt.close();
        return file;
    }

    private PrintWriter writeOutFileData(final File file)
            throws FileNotFoundException {
        return new PrintWriter(
                new OutputStreamWriter(new FileOutputStream(file),
                getUTF8Charset()));
    }

    private Charset getUTF8Charset() {
        return Charset.forName("UTF-8");
    }

    protected File updateFile(
            final File file, final Set<TestAccount> testAccounts)
            throws IOException {

        final BufferedWriter bufferedWriter = new BufferedWriter(
                new FileWriter(file, true));
        for (TestAccount user : testAccounts) {
            bufferedWriter.write(user.toLine(TestAccountsValue.FIELD_DELIMITER,
                    TestAccountsValue.TEXT_QUALIFIER));
            bufferedWriter.newLine();
        }
        bufferedWriter.flush();
        bufferedWriter.close();
        return file;
    }
}
