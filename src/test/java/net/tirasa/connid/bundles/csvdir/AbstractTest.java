/**
 * Copyright (C) 2011 ConnId (connid-dev@googlegroups.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.tirasa.connid.bundles.csvdir;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
import org.identityconnectors.test.common.TestHelpers;
import org.junit.jupiter.api.BeforeAll;

public abstract class AbstractTest {

    private static final boolean IGNORE_HEADER = false;

    protected static File testSourceDir;

    @BeforeAll
    public static void readTestProperties() {
        Properties props = new Properties();
        try {
            InputStream propStream = AbstractTest.class.getResourceAsStream("/test.properties");
            props.load(propStream);
            testSourceDir = Path.of(props.getProperty("testSourcePath")).toFile();
        } catch (Exception e) {
            fail("Could not load test.properties: " + e.getMessage());
        }
        assertNotNull(testSourceDir);
        assertTrue(testSourceDir.exists() ? testSourceDir.isDirectory() : testSourceDir.mkdir());
    }

    protected static CSVDirConfiguration createConfiguration(final String mask) {
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

    protected static CSVDirConfiguration createMultiOCsConfiguration(final String mask) {
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
            "OC",
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
        config.setObjectClassColumn("OC");
        config.setObjectClass(new String[] { "_EMPLOYEE_", "_MANAGER_" });
        return config;
    }

    protected static ConnectorFacade createMultiOCsFacade(final String mask) {
        final ConnectorFacadeFactory factory = ConnectorFacadeFactory.getInstance();

        final CSVDirConfiguration cfg = createMultiOCsConfiguration(mask);
        final APIConfiguration impl = TestHelpers.createTestConfiguration(CSVDirConnector.class, cfg);

        return factory.newInstance(impl);
    }

    protected static ConnectorFacade createFacade(final String mask) {
        final ConnectorFacadeFactory factory = ConnectorFacadeFactory.getInstance();

        final CSVDirConfiguration cfg = createConfiguration(mask);
        final APIConfiguration impl = TestHelpers.createTestConfiguration(CSVDirConnector.class, cfg);

        return factory.newInstance(impl);
    }

    protected static File createFile(final String name, final List<TestAccount> testAccounts) throws IOException {
        final File file = File.createTempFile(name, ".csv", testSourceDir);
        file.deleteOnExit();

        try (PrintWriter wrt = writeOutFileData(file.toPath())) {
            writeOutEachUser(wrt, testAccounts);
        }

        return file;
    }

    private static void writeOutEachUser(
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

    protected static File createSampleFile(final String name, final int thousands) throws IOException {
        final File file = File.createTempFile(name, ".csv", testSourceDir);
        file.deleteOnExit();

        try (PrintWriter wrt = writeOutFileData(file.toPath())) {
            if (IGNORE_HEADER) {
                wrt.println(TestAccountsValue.HEADER.toLine(
                        TestAccountsValue.FIELD_DELIMITER,
                        TestAccountsValue.TEXT_QUALIFIER));
            }

            for (int i = 0; i < thousands; i++) {
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
        }
        return file;
    }

    private static PrintWriter writeOutFileData(final Path file) throws IOException {
        return new PrintWriter(new OutputStreamWriter(Files.newOutputStream(file), StandardCharsets.UTF_8));
    }

    protected static File updateFile(final File file, final List<TestAccount> testAccounts) throws IOException {
        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(
                file.toPath(), StandardOpenOption.APPEND, StandardOpenOption.WRITE)) {

            for (TestAccount user : testAccounts) {
                bufferedWriter.write(user.toLine(
                        TestAccountsValue.FIELD_DELIMITER,
                        TestAccountsValue.TEXT_QUALIFIER));
                bufferedWriter.newLine();
            }
            bufferedWriter.flush();
        }
        return file;
    }

    protected static Set<Attribute> buildTestAttributes(final Name name) {
        final Set<Attribute> attributes = new HashSet<>();

        attributes.add(name);
        attributes.add(AttributeBuilder.build(TestAccountsValue.FIRSTNAME, "pmassi"));
        attributes.add(AttributeBuilder.build(TestAccountsValue.LASTNAME, "mperrone"));
        attributes.add(AttributeBuilder.build(TestAccountsValue.EMAIL, "massimiliano.perrone@test.it"));
        attributes.add(AttributeBuilder.build(TestAccountsValue.CHANGE_NUMBER, "0"));
        attributes.add(AttributeBuilder.build(TestAccountsValue.PASSWORD, "password"));
        attributes.add(AttributeBuilder.build(TestAccountsValue.DELETED, "no"));

        return attributes;
    }

    protected static Set<Attribute> setAccountId(final Set<Attribute> attributes) {
        attributes.add(AttributeBuilder.build(TestAccountsValue.ACCOUNTID, "___mperro123"));
        return attributes;
    }

    protected static Set<Attribute> setEmployee(final Set<Attribute> attributes) {
        attributes.add(AttributeBuilder.build("OC", "_EMPLOYEE_"));
        return attributes;
    }

    protected static Set<Attribute> setManager(final Set<Attribute> attributes) {
        attributes.add(AttributeBuilder.build("OC", "_MANAGER_"));
        return attributes;
    }
}
