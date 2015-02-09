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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationalAttributes;
import org.identityconnectors.framework.common.objects.ResultsHandler;
import org.identityconnectors.framework.common.objects.SyncDelta;
import org.identityconnectors.framework.common.objects.SyncDeltaType;
import org.identityconnectors.framework.common.objects.SyncResultsHandler;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.Filter;
import org.identityconnectors.framework.common.objects.filter.FilterBuilder;
import org.identityconnectors.framework.common.objects.filter.FilterVisitor;
import org.identityconnectors.test.common.TestHelpers;
import org.junit.Test;

public class CSVDirConnectorSyncTests extends AbstractTest {

    private static final double THOUSANDS = 0.1;

    private static class NoFilter implements Filter {

        @Override
        public boolean accept(final ConnectorObject obj) {
            return true;
        }

        @Override

        public <R, P> R accept(final FilterVisitor<R, P> v, final P p) {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    @Test

    public void search() throws IOException {
        createFile("sample", TestAccountsValue.TEST_ACCOUNTS);

        final CSVDirConnector connector = new CSVDirConnector();

        connector.init(createConfiguration("sample.*\\.csv"));

        // -----------------------
        // Filter definition
        // -----------------------
        final Filter firstnameFilter = FilterBuilder.startsWith(
                AttributeBuilder.build(TestAccountsValue.FIRSTNAME, "jPenelope"));

        final Filter surnameFilter = FilterBuilder.equalTo(
                AttributeBuilder.build(TestAccountsValue.LASTNAME, "jBacon"));

        final Filter filter = FilterBuilder.or(firstnameFilter, surnameFilter);

        // -----------------------
        final List<ConnectorObject> results = TestHelpers.searchToList(connector, ObjectClass.ACCOUNT, filter);

        final Set<TestAccount> actual = new HashSet<TestAccount>();

        for (ConnectorObject obj : results) {
            actual.add(new TestAccount(obj));
        }

        assertEquals(4, actual.size());

        connector.dispose();
    }

    @Test
    public void getConnectorObject() throws IOException {
        createFile("sample", TestAccountsValue.TEST_ACCOUNTS);

        // **test only**
        final ConnectorFacade facade = createFacade("sample.*\\.csv");

        final Uid uid = new Uid("____jpc4323435;jPenelope");

        final ConnectorObject object = facade.getObject(ObjectClass.ACCOUNT, uid, null);

        assertNotNull(object);
        assertEquals(object.getName().getNameValue(), uid.getUidValue());
        assertFalse((Boolean) object.getAttributeByName(OperationalAttributes.ENABLE_NAME).getValue().get(0));
    }

    @Test
    public void functional() throws IOException {
        createFile("sample", TestAccountsValue.TEST_ACCOUNTS);

        final Set<TestAccount> actual = new HashSet<TestAccount>();

        // **test only**
        final ConnectorFacade facade = createFacade("sample.*\\.csv");
        facade.search(ObjectClass.ACCOUNT, new NoFilter(), new ResultsHandler() {

            @Override
            public boolean handle(final ConnectorObject obj) {
                actual.add(new TestAccount(obj));
                return true;
            }
        }, null);

        // attempt to see if they compare..
        assertTrue(actual.containsAll(
                TestAccountsValue.TEST_ACCOUNTS.subList(0, TestAccountsValue.TEST_ACCOUNTS.size() - 1)));
    }

    @Test
    public final void syncWithNewFile() throws IOException {
        createFile("syncWithNewFile1", TestAccountsValue.TEST_ACCOUNTS);

        final CSVDirConnector connector = new CSVDirConnector();

        connector.init(createConfiguration("syncWithNewFile.*\\.csv"));

        final List<SyncDelta> syncDeltaList = new ArrayList<SyncDelta>();

        connector.sync(ObjectClass.ACCOUNT, new SyncToken(0), getSyncResultsHandler(syncDeltaList), null);

        assertEquals(syncDeltaList.size(), TestAccountsValue.TEST_ACCOUNTS.size());

        boolean found = false;

        for (SyncDelta delta : syncDeltaList) {

            ConnectorObject object = delta.getObject();

            if ("____jpc4323435;jPenelope".equals(object.getName().getNameValue())) {
                assertFalse((Boolean) object.getAttributeByName(OperationalAttributes.ENABLE_NAME).getValue().get(0));
                found = true;
            } else {
                assertTrue((Boolean) object.getAttributeByName(OperationalAttributes.ENABLE_NAME).getValue().get(0));
            }
        }

        assertTrue(found);

        createFile("syncWithNewFile2", TestAccountsValue.TEST_ACCOUNTS2);

        syncDeltaList.clear();

        connector.sync(ObjectClass.ACCOUNT, new SyncToken(0), getSyncResultsHandler(syncDeltaList), null);

        assertEquals(syncDeltaList.size(),
                TestAccountsValue.TEST_ACCOUNTS.size() + TestAccountsValue.TEST_ACCOUNTS2.size());

        connector.dispose();
    }

    @Test
    public final void syncWithUpdatedFile() throws IOException {
        File toBeUpdated = createFile("syncWithUpdatedFile", TestAccountsValue.TEST_ACCOUNTS);

        final CSVDirConnector connector = new CSVDirConnector();

        connector.init(createConfiguration("syncWithUpdatedFile.*\\.csv"));

        final List<SyncDelta> syncDeltaList = new ArrayList<SyncDelta>();

        connector.sync(ObjectClass.ACCOUNT, new SyncToken(0), getSyncResultsHandler(syncDeltaList), null);

        assertEquals(syncDeltaList.size(), TestAccountsValue.TEST_ACCOUNTS.size());

        updateFile(toBeUpdated, TestAccountsValue.TEST_ACCOUNTS2);

        syncDeltaList.clear();

        connector.sync(ObjectClass.ACCOUNT,
                new SyncToken(0),
                getSyncResultsHandler(syncDeltaList), null);

        assertEquals(syncDeltaList.size(),
                TestAccountsValue.TEST_ACCOUNTS.size() + TestAccountsValue.TEST_ACCOUNTS2.size());

        connector.dispose();
    }

    @Test
    public final void syncWithNoFilesForNewToken() throws IOException {
        createFile("syncWithNoFilesForNewToken", TestAccountsValue.TEST_ACCOUNTS);

        final CSVDirConnector connector = new CSVDirConnector();

        connector.init(createConfiguration("syncWithNoFilesForNewToken.*\\.csv"));

        final List<SyncDelta> syncDeltaList = new ArrayList<SyncDelta>();

        connector.sync(ObjectClass.ACCOUNT, new SyncToken(0), getSyncResultsHandler(syncDeltaList), null);

        assertEquals(TestAccountsValue.TEST_ACCOUNTS.size(), syncDeltaList.size());

        syncDeltaList.clear();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) {
            // ignore
        }

        connector.sync(ObjectClass.ACCOUNT,
                connector.getLatestSyncToken(ObjectClass.ACCOUNT),
                getSyncResultsHandler(syncDeltaList), null);

        assertEquals(syncDeltaList.size(), 0);

        connector.dispose();
    }

    @Test
    public final void syncWithNewFileAndRealToken() throws IOException {
        createFile("syncWithNewFileAndRealToken1", TestAccountsValue.TEST_ACCOUNTS);

        final CSVDirConnector connector = new CSVDirConnector();

        connector.init(createConfiguration("syncWithNewFileAndRealToken.*\\.csv"));

        final List<SyncDelta> syncDeltaList = new ArrayList<SyncDelta>();

        connector.sync(ObjectClass.ACCOUNT, new SyncToken(0), getSyncResultsHandler(syncDeltaList), null);

        assertEquals(syncDeltaList.size(), TestAccountsValue.TEST_ACCOUNTS.size());

        syncDeltaList.clear();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) {
            // ignore
        }

        createFile("syncWithNewFileAndRealToken2", TestAccountsValue.TEST_ACCOUNTS2);

        connector.sync(ObjectClass.ACCOUNT,
                connector.getLatestSyncToken(ObjectClass.ACCOUNT),
                getSyncResultsHandler(syncDeltaList), null);

        assertEquals(syncDeltaList.size(), +TestAccountsValue.TEST_ACCOUNTS2.size());

        connector.dispose();
    }

    @Test
    public final void syncWithUpdatedFileAndRealToken() throws IOException {
        File toBeUpdated = createFile("syncWithUpdatedFileAndRealToken", TestAccountsValue.TEST_ACCOUNTS);

        final CSVDirConnector connector = new CSVDirConnector();

        connector.init(createConfiguration("syncWithUpdatedFileAndRealToken.*\\.csv"));

        final List<SyncDelta> syncDeltaList = new ArrayList<SyncDelta>();

        connector.sync(ObjectClass.ACCOUNT, new SyncToken(0), getSyncResultsHandler(syncDeltaList), null);

        assertEquals(syncDeltaList.size(), TestAccountsValue.TEST_ACCOUNTS.size());

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

        assertEquals(syncDeltaList.size(),
                TestAccountsValue.TEST_ACCOUNTS.size() + TestAccountsValue.TEST_ACCOUNTS2.size());

        connector.dispose();
    }

    @Test
    public final void mixedOperations() throws IOException {
        File toBeUpdated = createFile("mixedOperations1", TestAccountsValue.TEST_ACCOUNTS);

        final CSVDirConnector connector = new CSVDirConnector();

        connector.init(createConfiguration("mixedOperations.*\\.csv"));

        final List<SyncDelta> syncDeltaList = new ArrayList<SyncDelta>();

        connector.sync(ObjectClass.ACCOUNT, new SyncToken(0),
                getSyncResultsHandler(syncDeltaList), null);

        assertEquals(syncDeltaList.size(), TestAccountsValue.TEST_ACCOUNTS.size());

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

        assertEquals(syncDeltaList.size(),
                TestAccountsValue.TEST_ACCOUNTS.size() + TestAccountsValue.TEST_ACCOUNTS2.size());

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

        assertEquals(syncDeltaList.size(), TestAccountsValue.TEST_ACCOUNTS3.size());

        syncDeltaList.clear();

        final Set<TestAccount> actual = new HashSet<TestAccount>();

        final List<ConnectorObject> results = TestHelpers.searchToList(connector, ObjectClass.ACCOUNT, new NoFilter());

        for (ConnectorObject obj : results) {
            actual.add(new TestAccount(obj));
        }

        final Set<TestAccount> accounts = new HashSet<TestAccount>(
                TestAccountsValue.TEST_ACCOUNTS.subList(0, TestAccountsValue.TEST_ACCOUNTS.size() - 1));

        accounts.addAll(TestAccountsValue.TEST_ACCOUNTS2);
        accounts.addAll(TestAccountsValue.TEST_ACCOUNTS3);

        assertEquals(accounts, actual);

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

        assertEquals(syncDeltaList.size(), (int) (THOUSANDS * 1000));

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
            taskExecutor.execute(new Runnable() {

                @Override
                public void run() {

                    try {

                        final List<SyncDelta> syncDeltaList = new ArrayList<SyncDelta>();

                        connector.sync(
                                ObjectClass.ACCOUNT,
                                new SyncToken(0),
                                getSyncResultsHandler(syncDeltaList),
                                null);

                        assertEquals(syncDeltaList.size(), 10);
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

        assertTrue(exception.toString(), exception.length() == 0);

        connector.dispose();
    }

    private SyncResultsHandler getSyncResultsHandler(final List<SyncDelta> list) {

        return new SyncResultsHandler() {

            @Override
            public boolean handle(final SyncDelta syncDelta) {
                list.add(syncDelta);
                return true;
            }
        };
    }

    @Test
    public void issueCSVDIR8() throws IOException {
        createFile("sample", TestAccountsValue.TEST_ACCOUNTS);

        final CSVDirConnector connector = new CSVDirConnector();

        connector.init(createConfiguration("sample.*\\.csv"));

        final List<ConnectorObject> results = TestHelpers.searchToList(connector, ObjectClass.ACCOUNT, null);

        assertEquals(8, results.size());

        connector.dispose();
    }

    @Test
    public final void incrementalSync() throws IOException {
        createFile("deleted", TestAccountsValue.TEST_ACCOUNTS);

        final CSVDirConnector connector = new CSVDirConnector();

        connector.init(createConfiguration("deleted.*\\.csv"));

        final List<SyncDelta> syncDeltaList = new ArrayList<SyncDelta>();

        connector.sync(ObjectClass.ACCOUNT, new SyncToken(0), getSyncResultsHandler(syncDeltaList), null);

        assertEquals(syncDeltaList.size(), TestAccountsValue.TEST_ACCOUNTS.size());

        boolean found = false;

        for (SyncDelta delta : syncDeltaList) {
            ConnectorObject object = delta.getObject();

            if ("____deletedUser@bob.com;deletedUser".equals(object.getName().getNameValue())) {
                assertEquals(SyncDeltaType.DELETE, delta.getDeltaType());
                found = true;
            }
        }

        assertTrue(found);
        connector.dispose();
    }
}
