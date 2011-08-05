/*
 * ====================
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 * 
 * Copyright 2008-2009 Sun Microsystems, Inc. All rights reserved.     
 * 
 * The contents of this file are subject to the terms of the Common Development 
 * and Distribution License("CDDL") (the "License").  You may not use this file 
 * except in compliance with the License.
 * 
 * You can obtain a copy of the License at 
 * http://IdentityConnectors.dev.java.net/legal/license.txt
 * See the License for the specific language governing permissions and limitations 
 * under the License. 
 * 
 * When distributing the Covered Code, include this CDDL Header Notice in each file
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.identityconnectors.framework.api.APIConfiguration;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.api.ConnectorFacadeFactory;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.ResultsHandler;
import org.identityconnectors.framework.common.objects.SyncDelta;
import org.identityconnectors.framework.common.objects.SyncResultsHandler;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.Filter;
import org.identityconnectors.framework.common.objects.filter.FilterBuilder;
import org.identityconnectors.test.common.TestHelpers;
import org.junit.Assert;
import org.junit.Test;

public class CSVDirConnectorTests {

    private static double THOUSANDS = 0.1;

    static class NoFilter implements Filter {

        @Override
        public boolean accept(final ConnectorObject obj) {
            return true;
        }
    }

    private Charset getUTF8Charset() {
        return Charset.forName("UTF-8");
    }

    @Test
    public void search() throws IOException {
        createFile("sample", TestAccountsValue.TEST_ACCOUNTS);

        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(
                createConfiguration("sample.*\\.csv"));

        // -----------------------
        // Filter definition
        // -----------------------
        Filter firstnameFilter = FilterBuilder.startsWith(
                AttributeBuilder.build(
                TestAccountsValue.FIRSTNAME, "jPenelope"));

        Filter surnameFilter = FilterBuilder.equalTo(
                AttributeBuilder.build(
                TestAccountsValue.LASTNAME, "jBacon"));

        Filter filter = FilterBuilder.or(
                firstnameFilter, surnameFilter);
        // -----------------------

        final Set<TestAccount> actual = new HashSet<TestAccount>();

        final List<ConnectorObject> results = TestHelpers.searchToList(
                connector, ObjectClass.ACCOUNT, filter);

        for (ConnectorObject obj : results) {
            actual.add(new TestAccount(obj));
        }

        Assert.assertEquals(actual.size(), 4);

        connector.dispose();
    }

    @Test
    public void getConnectorObject() throws IOException {

        createFile("sample", TestAccountsValue.TEST_ACCOUNTS);

        final ConnectorFacadeFactory factory =
                ConnectorFacadeFactory.getInstance();

        // **test only**
        final APIConfiguration impl = TestHelpers.createTestConfiguration(
                CSVDirConnector.class, createConfiguration("sample.*\\.csv"));

        final ConnectorFacade facade = factory.newInstance(impl);

        Uid uid = new Uid("____jpc4323435,jPenelope");

        ConnectorObject object = facade.getObject(
                ObjectClass.ACCOUNT, uid, null);

        Assert.assertNotNull(object);
        Assert.assertEquals(object.getName().getNameValue(), uid.getUidValue());
    }

    @Test
    public void functional() throws IOException {

        createFile("sample", TestAccountsValue.TEST_ACCOUNTS);

        final Set<TestAccount> actual = new HashSet<TestAccount>();

        final ConnectorFacadeFactory factory =
                ConnectorFacadeFactory.getInstance();

        // **test only**
        final APIConfiguration impl = TestHelpers.createTestConfiguration(
                CSVDirConnector.class, createConfiguration("sample.*\\.csv"));

        final ConnectorFacade facade = factory.newInstance(impl);

        facade.search(ObjectClass.ACCOUNT, new NoFilter(),
                new ResultsHandler() {

                    @Override
                    public boolean handle(final ConnectorObject obj) {
                        actual.add(new TestAccount(obj));
                        return true;
                    }
                }, null);

        // attempt to see if they compare..
        Assert.assertEquals(TestAccountsValue.TEST_ACCOUNTS, actual);
    }

    @Test
    public final void syncWithNewFile() throws IOException {
        createFile("syncWithNewFile1", TestAccountsValue.TEST_ACCOUNTS);

        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(createConfiguration("syncWithNewFile.*\\.csv"));

        final List<SyncDelta> syncDeltaList = new ArrayList<SyncDelta>();

        connector.sync(ObjectClass.ACCOUNT, new SyncToken(0),
                getSyncResultsHandler(syncDeltaList), null);

        Assert.assertEquals(syncDeltaList.size(),
                TestAccountsValue.TEST_ACCOUNTS.size());

        createFile("syncWithNewFile2", TestAccountsValue.TEST_ACCOUNTS2);

        syncDeltaList.clear();

        connector.sync(ObjectClass.ACCOUNT, new SyncToken(0),
                getSyncResultsHandler(syncDeltaList), null);

        Assert.assertEquals(syncDeltaList.size(),
                TestAccountsValue.TEST_ACCOUNTS.size()
                + TestAccountsValue.TEST_ACCOUNTS2.size());

        connector.dispose();
    }

    @Test
    public final void syncWithUpdatedFile() throws IOException {
        File toBeUpdated = createFile(
                "syncWithUpdatedFile", TestAccountsValue.TEST_ACCOUNTS);

        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(createConfiguration("syncWithUpdatedFile.*\\.csv"));

        final List<SyncDelta> syncDeltaList = new ArrayList<SyncDelta>();

        connector.sync(ObjectClass.ACCOUNT, new SyncToken(0),
                getSyncResultsHandler(syncDeltaList), null);

        Assert.assertEquals(syncDeltaList.size(),
                TestAccountsValue.TEST_ACCOUNTS.size());

        updateFile(toBeUpdated, TestAccountsValue.TEST_ACCOUNTS2);

        syncDeltaList.clear();

        connector.sync(ObjectClass.ACCOUNT,
                new SyncToken(0),
                getSyncResultsHandler(syncDeltaList), null);

        Assert.assertEquals(syncDeltaList.size(),
                TestAccountsValue.TEST_ACCOUNTS.size()
                + TestAccountsValue.TEST_ACCOUNTS2.size());

        connector.dispose();
    }

    @Test
    public final void syncWithNoFilesForNewToken() throws IOException {
        createFile("syncWithNoFilesForNewToken",
                TestAccountsValue.TEST_ACCOUNTS);

        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(
                createConfiguration("syncWithNoFilesForNewToken.*\\.csv"));

        final List<SyncDelta> syncDeltaList = new ArrayList<SyncDelta>();

        connector.sync(ObjectClass.ACCOUNT, new SyncToken(0),
                getSyncResultsHandler(syncDeltaList), null);

        Assert.assertEquals(syncDeltaList.size(),
                TestAccountsValue.TEST_ACCOUNTS.size());

        syncDeltaList.clear();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) {
            // ignore
        }

        connector.sync(ObjectClass.ACCOUNT,
                connector.getLatestSyncToken(ObjectClass.ACCOUNT),
                getSyncResultsHandler(syncDeltaList), null);


        Assert.assertEquals(syncDeltaList.size(), 0);

        connector.dispose();
    }

    @Test
    public final void syncWithNewFileAndRealToken() throws IOException {
        createFile("syncWithNewFileAndRealToken1",
                TestAccountsValue.TEST_ACCOUNTS);

        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(
                createConfiguration("syncWithNewFileAndRealToken.*\\.csv"));

        final List<SyncDelta> syncDeltaList = new ArrayList<SyncDelta>();

        connector.sync(ObjectClass.ACCOUNT, new SyncToken(0),
                getSyncResultsHandler(syncDeltaList), null);

        Assert.assertEquals(syncDeltaList.size(),
                TestAccountsValue.TEST_ACCOUNTS.size());

        syncDeltaList.clear();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) {
            // ignore
        }

        createFile("syncWithNewFileAndRealToken2",
                TestAccountsValue.TEST_ACCOUNTS2);

        connector.sync(ObjectClass.ACCOUNT,
                connector.getLatestSyncToken(ObjectClass.ACCOUNT),
                getSyncResultsHandler(syncDeltaList), null);

        Assert.assertEquals(syncDeltaList.size(),
                +TestAccountsValue.TEST_ACCOUNTS2.size());

        connector.dispose();
    }

    @Test
    public final void syncWithUpdatedFileAndRealToken() throws IOException {
        File toBeUpdated = createFile(
                "syncWithUpdatedFileAndRealToken",
                TestAccountsValue.TEST_ACCOUNTS);

        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(createConfiguration(
                "syncWithUpdatedFileAndRealToken.*\\.csv"));

        final List<SyncDelta> syncDeltaList = new ArrayList<SyncDelta>();

        connector.sync(ObjectClass.ACCOUNT, new SyncToken(0),
                getSyncResultsHandler(syncDeltaList), null);

        Assert.assertEquals(syncDeltaList.size(),
                TestAccountsValue.TEST_ACCOUNTS.size());

        syncDeltaList.clear();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) {
            // ignore
        }

        updateFile(toBeUpdated, TestAccountsValue.TEST_ACCOUNTS2);

        connector.sync(ObjectClass.ACCOUNT,
                connector.getLatestSyncToken(ObjectClass.ACCOUNT),
                getSyncResultsHandler(syncDeltaList), null);

        Assert.assertEquals(syncDeltaList.size(),
                TestAccountsValue.TEST_ACCOUNTS.size()
                + TestAccountsValue.TEST_ACCOUNTS2.size());

        connector.dispose();
    }

    @Test
    public final void mixedOperations() throws IOException {
        File toBeUpdated = createFile(
                "mixedOperations1",
                TestAccountsValue.TEST_ACCOUNTS);

        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(createConfiguration(
                "mixedOperations.*\\.csv"));

        final List<SyncDelta> syncDeltaList = new ArrayList<SyncDelta>();

        connector.sync(ObjectClass.ACCOUNT, new SyncToken(0),
                getSyncResultsHandler(syncDeltaList), null);

        Assert.assertEquals(syncDeltaList.size(),
                TestAccountsValue.TEST_ACCOUNTS.size());

        syncDeltaList.clear();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) {
            // ignore
        }

        updateFile(toBeUpdated, TestAccountsValue.TEST_ACCOUNTS2);

        connector.sync(ObjectClass.ACCOUNT,
                connector.getLatestSyncToken(ObjectClass.ACCOUNT),
                getSyncResultsHandler(syncDeltaList), null);

        Assert.assertEquals(syncDeltaList.size(),
                TestAccountsValue.TEST_ACCOUNTS.size()
                + TestAccountsValue.TEST_ACCOUNTS2.size());

        syncDeltaList.clear();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) {
            // ignore
        }

        createFile("mixedOperations2", TestAccountsValue.TEST_ACCOUNTS3);

        connector.sync(ObjectClass.ACCOUNT,
                connector.getLatestSyncToken(ObjectClass.ACCOUNT),
                getSyncResultsHandler(syncDeltaList), null);

        Assert.assertEquals(syncDeltaList.size(),
                TestAccountsValue.TEST_ACCOUNTS3.size());

        syncDeltaList.clear();

        final Set<TestAccount> actual = new HashSet<TestAccount>();

        final List<ConnectorObject> results = TestHelpers.searchToList(
                connector, ObjectClass.ACCOUNT, new NoFilter());

        for (ConnectorObject obj : results) {
            actual.add(new TestAccount(obj));
        }

        Set<TestAccount> accounts =
                new HashSet(TestAccountsValue.TEST_ACCOUNTS);
        accounts.addAll(TestAccountsValue.TEST_ACCOUNTS2);
        accounts.addAll(TestAccountsValue.TEST_ACCOUNTS3);

        Assert.assertEquals(accounts, actual);

        connector.dispose();
    }

    @Test
    public final void syncThousandsEntries() throws IOException {
        createSampleFile("thousands", (int) (THOUSANDS * 1000));

        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(createConfiguration("thousands.*\\.csv"));

        final List<SyncDelta> syncDeltaList = new ArrayList<SyncDelta>();

        connector.sync(ObjectClass.ACCOUNT, new SyncToken(0),
                getSyncResultsHandler(syncDeltaList), null);

        Assert.assertEquals(syncDeltaList.size(), (int) (THOUSANDS * 1000));

        connector.dispose();
    }

    @Test
    public final void concurrentOperations() throws IOException {
        final CSVDirConnector connector = new CSVDirConnector();
        connector.init(createConfiguration("concurrentOperations.*\\.csv"));
        createSampleFile("concurrentOperations", 10);

        final ExecutorService taskExecutor = Executors.newFixedThreadPool(10);

        final StringBuilder exception = new StringBuilder();

        for (int i = 0; i < 10; i++) {
            taskExecutor.execute(
                    new Runnable() {

                        @Override
                        public void run() {

                            try {

                                final List<SyncDelta> syncDeltaList =
                                        new ArrayList<SyncDelta>();

                                connector.sync(
                                        ObjectClass.ACCOUNT,
                                        new SyncToken(0),
                                        getSyncResultsHandler(syncDeltaList),
                                        null);

                                Assert.assertEquals(syncDeltaList.size(), 10);

                            } catch (Throwable t) {
                                exception.append(t.getMessage()).append("\n");
                            }
                        }
                    });
        }

        taskExecutor.shutdown();

        try {
            taskExecutor.awaitTermination(10L, TimeUnit.SECONDS);
        } catch (InterruptedException ignore) {
            // ignore exception
        }

        Assert.assertTrue(exception.toString(), exception.length() == 0);

        connector.dispose();
    }

    private SyncResultsHandler getSyncResultsHandler(final List list) {
        return new SyncResultsHandler() {

            @Override
            public boolean handle(final SyncDelta syncDelta) {
                list.add(syncDelta);
                return true;
            }
        };
    }

    private CSVDirConfiguration createConfiguration(
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

    private File createFile(
            final String name, final Set<TestAccount> testAccounts)
            throws IOException {
        final File file = File.createTempFile(name, ".csv");
        file.deleteOnExit();

        final PrintWriter wrt = writeOutFileData(file);
        writeOutEachUser(wrt, testAccounts);
        wrt.close();
        return file;
    }

    private File createSampleFile(String name, int THOUSANDS)
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

    private File updateFile(
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

    private PrintWriter writeOutFileData(final File file) throws FileNotFoundException {
        return new PrintWriter(
                new OutputStreamWriter(new FileOutputStream(file),
                getUTF8Charset()));
    }

    private void writeOutEachUser(final PrintWriter wrt,
            final Set<TestAccount> testAccounts) {
        wrt.println(TestAccountsValue.HEADER.toLine(TestAccountsValue.FIELD_DELIMITER,
                TestAccountsValue.TEXT_QUALIFIER));
        for (TestAccount user : testAccounts) {
            wrt.println(user.toLine(TestAccountsValue.FIELD_DELIMITER,
                    TestAccountsValue.TEXT_QUALIFIER));
        }
    }
}
