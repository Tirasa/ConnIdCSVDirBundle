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

import java.util.ArrayList;
import java.util.List;

public class TestAccountsValue {

    public static final String CHANGE_NUMBER = "changeNumber";

    public static final String ACCOUNTID = "accountid";

    public static final String PASSWORD = "password";

    public static final String DELETED = "deleted";

    public static final String FIRSTNAME = "firstname";

    public static final String LASTNAME = "lastname";

    public static final String EMAIL = "email";

    public static final String STATUS = "status";

    public static final char FIELD_DELIMITER = ',';

    public static final char TEXT_QUALIFIER = '"';

    public static final TestAccount HEADER =
            new TestAccount(ACCOUNTID, FIRSTNAME,
                    LASTNAME, EMAIL, CHANGE_NUMBER, PASSWORD, DELETED);

    public static final List<TestAccount> TEST_ACCOUNTS = new ArrayList<TestAccount>();

    public static final List<TestAccount> TEST_ACCOUNTS2 = new ArrayList<TestAccount>();

    public static final List<TestAccount> TEST_ACCOUNTS3 = new ArrayList<TestAccount>();

    public static final List<TestAccount> TEST_ACCOUNTS4 = new ArrayList<TestAccount>();

    static {
        TEST_ACCOUNTS.add(new TestAccount("____jpc4323435", "jPenelope",
                "jCruz", "jxPenelope.Cruz@mail.com", "0", "password", "false", "false"));
        TEST_ACCOUNTS.add(new TestAccount("____jkb3234416", "jKevin", "jBacon",
                "jxKevin.Bacon@mail.com", "1", "password", "false"));
        TEST_ACCOUNTS.add(new TestAccount("____jpc4323436", "jPenelope",
                "jCruz2", "jyPenelope.Cruz@mail.com", "2", "password", "false"));
        TEST_ACCOUNTS.add(new TestAccount("____jkb3234417", "jKevin",
                "jBacon,II", "jyKevin.Bacon@mail.com", "3", "password", "false"));
        TEST_ACCOUNTS.add(new TestAccount("____jpc4323437", "jPenelope",
                "jCruz3", "jzPenelope.Cruz@mail.com", "4", "password", "false"));
        TEST_ACCOUNTS.add(new TestAccount("____jkb3234419", "jKevin",
                "jBacon,III", "jzKevin.Bacon@mail.com", "5", "password", "false"));
        TEST_ACCOUNTS.add(new TestAccount("____billy@bob.com", "jBilly", "jBob",
                "jaBilly.Bob@mail.com", "6", "password", "false"));
        TEST_ACCOUNTS.add(new TestAccount("____bil@bob@bob.com", "jBillyBob",
                "jBobby", "jaBillyBob.Bobby@mail.com", "7", "password", "false"));
        TEST_ACCOUNTS.add(new TestAccount("____deletedUser@bob.com", "deletedUser",
                "deletedUser", "deletedUser@mail.com", "8", "password", "true"));
    }

    static {
        TEST_ACCOUNTS2.add(new TestAccount("___jpc4323435", "jAl", "jPacino",
                "jxPenelope.Cruz@mail.com", "0", "password", "false"));
        TEST_ACCOUNTS2.add(new TestAccount("___jkb3234416", "jAl", "jCapone",
                "jxKevin.Bacon@mail.com", "1", "password", "false"));
        TEST_ACCOUNTS2.add(new TestAccount("___jpc4323436", "jPenelope",
                "jPacino2", "jyPenelope.Cruz@mail.com", "2", "password", "false"));
        TEST_ACCOUNTS2.add(new TestAccount("___jkb3234417", "jKevin",
                "jCapone,II", "jyKevin.Bacon@mail.com", "3", "password", "false"));
        TEST_ACCOUNTS2.add(new TestAccount("___jpc4323437", "jPenelope",
                "jPacino3", "jzPenelope.Cruz@mail.com", "4", "password", "false"));
        TEST_ACCOUNTS2.add(new TestAccount("___jkb3234419", "jKevin",
                "jCapone,III", "jzKevin.Bacon@mail.com",
                "5", "password", "false"));
        TEST_ACCOUNTS2.add(new TestAccount("___billy@bob.com", "jBilly",
                "jBobASDASD", "jaBilly.Bob@mail.com", "6", "password", "false"));
        TEST_ACCOUNTS2.add(new TestAccount("___bil@bob@bob.com", "jBillyBoaab",
                "jBobby", "jaBillyBob.Bobby@mail.com", "7", "password", "false"));
    }

    static {
        TEST_ACCOUNTS3.add(new TestAccount("__jpc4323435", "jAl", "jBano",
                "jxPenelope.Cruz@mail.com", "0", "password", "false"));
        TEST_ACCOUNTS3.add(new TestAccount("__jkb3234416", "jAl", "jJazeera",
                "jxKevin.Bacon@mail.com", "1", "password", "false"));
        TEST_ACCOUNTS3.add(new TestAccount("__jpc4323436", "jPenelope",
                "jBano2", "jyPenelope.Cruz@mail.com", "2", "password", "false"));
        TEST_ACCOUNTS3.add(new TestAccount("__jkb3234417", "jKevin",
                "jJazeera,II", "jyKevin.Bacon@mail.com", "3",
                "password", "false"));
        TEST_ACCOUNTS3.add(new TestAccount("__jpc4323437", "jPenelope", "jBano3",
                "jzPenelope.Cruz@mail.com", "4", "password", "false"));
        TEST_ACCOUNTS3.add(new TestAccount("__jkb3234419", "jKevin",
                "jJazzera,III", "jzKevin.Bacon@mail.com", "5",
                "password", "false"));
        TEST_ACCOUNTS3.add(new TestAccount("__billy@bob.com", "jBilly",
                "jBobMax", "jaBilly.Bob@mail.com", "6", "password", "false"));
        TEST_ACCOUNTS3.add(new TestAccount("__bil@bob@bob.com", "jBillyBobMin",
                "jBobby", "jaBillyBob.Bobby@mail.com", "7", "password", "false"));
    }

    static {
        TEST_ACCOUNTS4.add(new TestAccount("_jpc4323435", "jAl", "jMarco",
                "jxPenelope.Cruz@mail.com", "0", "password", "false"));
        TEST_ACCOUNTS4.add(new TestAccount("_jkb3234416", "jAl", "jFrancesco",
                "jxKevin.Bacon@mail.com", "1", "password", "false"));
        TEST_ACCOUNTS4.add(new TestAccount("_jpc4323436", "jPenelope",
                "jMarco2", "jyPenelope.Cruz@mail.com", "2", "password", "false"));
        TEST_ACCOUNTS4.add(new TestAccount("_jkb3234417", "jKevin",
                "jFrancesco,II", "jyKevin.Bacon@mail.com", "3",
                "password", "false"));
        TEST_ACCOUNTS4.add(new TestAccount("_jpc4323437", "jPenelope",
                "jFabio3", "jzPenelope.Cruz@mail.com", "4", "password", "false"));
        TEST_ACCOUNTS4.add(new TestAccount("_jkb3234419", "jKevin",
                "jCinzia,III", "jzKevin.Bacon@mail.com", "5",
                "password", "false"));
    }
}
