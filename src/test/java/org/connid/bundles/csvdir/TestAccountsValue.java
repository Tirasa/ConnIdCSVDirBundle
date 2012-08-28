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
 * https://connid.googlecode.com/svn/base/trunk/legal/license.txt
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
package org.connid.bundles.csvdir;

import java.util.HashSet;
import java.util.Set;

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

    public static final Set<TestAccount> TEST_ACCOUNTS =
            new HashSet<TestAccount>();

    public static final Set<TestAccount> TEST_ACCOUNTS2 =
            new HashSet<TestAccount>();

    public static final Set<TestAccount> TEST_ACCOUNTS3 =
            new HashSet<TestAccount>();

    public static final Set<TestAccount> TEST_ACCOUNTS4 =
            new HashSet<TestAccount>();

    static {
        TEST_ACCOUNTS.add(new TestAccount("____jpc4323435", "jPenelope",
                "jCruz", "jxPenelope.Cruz@mail.com", "0", "password", "no", "false"));
        TEST_ACCOUNTS.add(new TestAccount("____jkb3234416", "jKevin", "jBacon",
                "jxKevin.Bacon@mail.com", "1", "password", "no"));
        TEST_ACCOUNTS.add(new TestAccount("____jpc4323436", "jPenelope",
                "jCruz2", "jyPenelope.Cruz@mail.com", "2", "password", "no"));
        TEST_ACCOUNTS.add(new TestAccount("____jkb3234417", "jKevin",
                "jBacon,II", "jyKevin.Bacon@mail.com", "3", "password", "no"));
        TEST_ACCOUNTS.add(new TestAccount("____jpc4323437", "jPenelope",
                "jCruz3", "jzPenelope.Cruz@mail.com", "4", "password", "no"));
        TEST_ACCOUNTS.add(new TestAccount("____jkb3234419", "jKevin",
                "jBacon,III", "jzKevin.Bacon@mail.com", "5", "password", "no"));
        TEST_ACCOUNTS.add(new TestAccount("____billy@bob.com", "jBilly", "jBob",
                "jaBilly.Bob@mail.com", "6", "password", "no"));
        TEST_ACCOUNTS.add(new TestAccount("____bil@bob@bob.com", "jBillyBob",
                "jBobby", "jaBillyBob.Bobby@mail.com", "7", "password", "no"));
    }

    static {
        TEST_ACCOUNTS2.add(new TestAccount("___jpc4323435", "jAl", "jPacino",
                "jxPenelope.Cruz@mail.com", "0", "password", "no"));
        TEST_ACCOUNTS2.add(new TestAccount("___jkb3234416", "jAl", "jCapone",
                "jxKevin.Bacon@mail.com", "1", "password", "no"));
        TEST_ACCOUNTS2.add(new TestAccount("___jpc4323436", "jPenelope",
                "jPacino2", "jyPenelope.Cruz@mail.com", "2", "password", "no"));
        TEST_ACCOUNTS2.add(new TestAccount("___jkb3234417", "jKevin",
                "jCapone,II", "jyKevin.Bacon@mail.com", "3", "password", "no"));
        TEST_ACCOUNTS2.add(new TestAccount("___jpc4323437", "jPenelope",
                "jPacino3", "jzPenelope.Cruz@mail.com", "4", "password", "no"));
        TEST_ACCOUNTS2.add(new TestAccount("___jkb3234419", "jKevin",
                "jCapone,III", "jzKevin.Bacon@mail.com",
                "5", "password", "no"));
        TEST_ACCOUNTS2.add(new TestAccount("___billy@bob.com", "jBilly",
                "jBobASDASD", "jaBilly.Bob@mail.com", "6", "password", "no"));
        TEST_ACCOUNTS2.add(new TestAccount("___bil@bob@bob.com", "jBillyBoaab",
                "jBobby", "jaBillyBob.Bobby@mail.com", "7", "password", "no"));
    }

    static {
        TEST_ACCOUNTS3.add(new TestAccount("__jpc4323435", "jAl", "jBano",
                "jxPenelope.Cruz@mail.com", "0", "password", "no"));
        TEST_ACCOUNTS3.add(new TestAccount("__jkb3234416", "jAl", "jJazeera",
                "jxKevin.Bacon@mail.com", "1", "password", "no"));
        TEST_ACCOUNTS3.add(new TestAccount("__jpc4323436", "jPenelope",
                "jBano2", "jyPenelope.Cruz@mail.com", "2", "password", "no"));
        TEST_ACCOUNTS3.add(new TestAccount("__jkb3234417", "jKevin",
                "jJazeera,II", "jyKevin.Bacon@mail.com", "3",
                "password", "no"));
        TEST_ACCOUNTS3.add(new TestAccount("__jpc4323437", "jPenelope", "jBano3",
                "jzPenelope.Cruz@mail.com", "4", "password", "no"));
        TEST_ACCOUNTS3.add(new TestAccount("__jkb3234419", "jKevin",
                "jJazzera,III", "jzKevin.Bacon@mail.com", "5",
                "password", "no"));
        TEST_ACCOUNTS3.add(new TestAccount("__billy@bob.com", "jBilly",
                "jBobMax", "jaBilly.Bob@mail.com", "6", "password", "no"));
        TEST_ACCOUNTS3.add(new TestAccount("__bil@bob@bob.com", "jBillyBobMin",
                "jBobby", "jaBillyBob.Bobby@mail.com", "7", "password", "no"));
    }

    static {
        TEST_ACCOUNTS4.add(new TestAccount("_jpc4323435", "jAl", "jMarco",
                "jxPenelope.Cruz@mail.com", "0", "password", "no"));
        TEST_ACCOUNTS4.add(new TestAccount("_jkb3234416", "jAl", "jFrancesco",
                "jxKevin.Bacon@mail.com", "1", "password", "no"));
        TEST_ACCOUNTS4.add(new TestAccount("_jpc4323436", "jPenelope",
                "jMarco2", "jyPenelope.Cruz@mail.com", "2", "password", "no"));
        TEST_ACCOUNTS4.add(new TestAccount("_jkb3234417", "jKevin",
                "jFrancesco,II", "jyKevin.Bacon@mail.com", "3",
                "password", "no"));
        TEST_ACCOUNTS4.add(new TestAccount("_jpc4323437", "jPenelope",
                "jFabio3", "jzPenelope.Cruz@mail.com", "4", "password", "no"));
        TEST_ACCOUNTS4.add(new TestAccount("_jkb3234419", "jKevin",
                "jCinzia,III", "jzKevin.Bacon@mail.com", "5",
                "password", "no"));
    }
}
