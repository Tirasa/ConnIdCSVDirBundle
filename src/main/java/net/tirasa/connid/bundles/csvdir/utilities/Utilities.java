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
package net.tirasa.connid.bundles.csvdir.utilities;

import java.security.SecureRandom;
import java.util.Collection;

public final class Utilities {

    public static final String EMPTY = "";

    private static final SecureRandom RANDOM = new SecureRandom();

    private Utilities() {
        // empty method for static utility class
    }

    public static int randomNumber() {
        return RANDOM.nextInt(100000);
    }

    public static String join(final Collection<String> collection, final char separator) {
        if (collection == null) {
            return null;
        }

        return join(collection.toArray(new String[collection.size()]), separator, 0, collection.size());
    }

    public static String join(final Object[] array, final char separator) {
        if (array == null) {
            return null;
        }

        return join(array, separator, 0, array.length);
    }

    public static String join(final Object[] array, final char separator, final int startIndex, final int endIndex) {
        if (array == null) {
            return null;
        }
        final int noOfItems = endIndex - startIndex;
        if (noOfItems <= 0) {
            return EMPTY;
        }

        final StringBuilder buf = new StringBuilder(noOfItems * 16);

        for (int i = startIndex; i < endIndex; i++) {
            if (i > startIndex) {
                buf.append(separator);
            }
            if (array[i] != null) {
                buf.append(array[i]);
            }
        }
        return buf.toString();
    }
}
