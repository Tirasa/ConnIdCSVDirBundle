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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.identityconnectors.framework.api.APIConfiguration;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.api.ConnectorFacadeFactory;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.impl.api.APIConfigurationImpl;
import org.identityconnectors.framework.impl.api.local.JavaClassProperties;
import org.identityconnectors.test.common.TestHelpers;
import org.junit.Before;

public abstract class AbstractTest {

    private static boolean IGNORE_HEADER = false;

    protected File testSourceDir;

    @Before
    public void readTestProperties() {
        Properties props = new Properties();
        try {
            InputStream propStream = getClass().getResourceAsStream("/test.properties");
            props.load(propStream);
            testSourceDir = new File(props.getProperty("testSourcePath"));
        } catch (Exception e) {
            fail("Could not load test.properties: " + e.getMessage());
        }
        assertNotNull(testSourceDir);
        assertTrue(testSourceDir.exists() ? testSourceDir.isDirectory() : testSourceDir.mkdir());
    }

    protected CSVDirConfiguration createConfiguration(final String mask) {
        // create the connector configuration..
        final CSVDirConfiguration config = new CSVDirConfiguration();
        config.setFileMask(mask);
        config.setKeyColumnNames(new String[] { TestAccountsValue.ACCOUNTID, TestAccountsValue.FIRSTNAME });
        config.setDeleteColumnName(TestAccountsValue.DELETED);
        config.setPasswordColumnName(TestAccountsValue.PASSWORD);
        config.setSourcePath(testSourceDir.getPath());
        config.setQuotationRequired(Boolean.TRUE);
        config.setIgnoreHeader(IGNORE_HEADER);
        config.setKeyseparator(";");
        config.setFields(new String[] {
            TestAccountsValue.ACCOUNTID,
            TestAccountsValue.FIRSTNAME,
            TestAccountsValue.LASTNAME,
            TestAccountsValue.EMAIL,
            TestAccountsValue.CHANGE_NUMBER,
            TestAccountsValue.PASSWORD,
            TestAccountsValue.DELETED,
            TestAccountsValue.STATUS });
        config.setStatusColumn("status");
        config.validate();
        return config;
    }

    protected ConnectorFacade createFacade(final String mask) {
        final ConnectorFacadeFactory factory = ConnectorFacadeFactory.getInstance();

        final CSVDirConfiguration cfg = createConfiguration(mask);
        final APIConfiguration impl = TestHelpers.createTestConfiguration(CSVDirConnector.class, cfg);
        // TODO: remove the line below when using ConnId >= 1.4.0.1
        ((APIConfigurationImpl) impl).
                setConfigurationProperties(JavaClassProperties.createConfigurationProperties(cfg));

        return factory.newInstance(impl);
    }

    protected File createFile(final String name, final List<TestAccount> testAccounts)
            throws IOException {

        final File file = File.createTempFile(name, ".csv", testSourceDir);
        file.deleteOnExit();

        final PrintWriter wrt = writeOutFileData(file);
        writeOutEachUser(wrt, testAccounts);
        wrt.close();

        return file;
    }

    private void writeOutEachUser(
            final PrintWriter wrt,
            final List<TestAccount> testAccounts) {

        if (IGNORE_HEADER) {
            wrt.println(TestAccountsValue.HEADER.toLine(
                    TestAccountsValue.FIELD_DELIMITER,
                    TestAccountsValue.TEXT_QUALIFIER));
        }

        for (TestAccount user : testAccounts) {
            wrt.println(user.toLine(
                    TestAccountsValue.FIELD_DELIMITER,
                    TestAccountsValue.TEXT_QUALIFIER));
        }
    }

    protected File createSampleFile(final String name, final int THOUSANDS)
            throws IOException {

        final File file = File.createTempFile(name, ".csv", testSourceDir);
        file.deleteOnExit();

        final PrintWriter wrt = writeOutFileData(file);

        if (IGNORE_HEADER) {
            wrt.println(TestAccountsValue.HEADER.toLine(
                    TestAccountsValue.FIELD_DELIMITER,
                    TestAccountsValue.TEXT_QUALIFIER));
        }

        for (int i = 0; i < THOUSANDS; i++) {
            TestAccount account = new TestAccount(
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
        return new PrintWriter(new OutputStreamWriter(
                new FileOutputStream(file), getUTF8Charset()));
    }

    private Charset getUTF8Charset() {
        return Charset.forName("UTF-8");
    }

    protected File updateFile(final File file, final List<TestAccount> testAccounts)
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

    protected Set<Attribute> buildTestAttributes(final Name name) {
        final Set<Attribute> attributes = new HashSet<Attribute>();

        attributes.add(name);
        attributes.add(AttributeBuilder.build(TestAccountsValue.FIRSTNAME, "pmassi"));
        attributes.add(AttributeBuilder.build(TestAccountsValue.LASTNAME, "mperrone"));
        attributes.add(AttributeBuilder.build(TestAccountsValue.EMAIL, "massimiliano.perrone@test.it"));
        attributes.add(AttributeBuilder.build(TestAccountsValue.CHANGE_NUMBER, "0"));
        attributes.add(AttributeBuilder.build(TestAccountsValue.PASSWORD, "password"));
        attributes.add(AttributeBuilder.build(TestAccountsValue.DELETED, "no"));

        return attributes;
    }
}
