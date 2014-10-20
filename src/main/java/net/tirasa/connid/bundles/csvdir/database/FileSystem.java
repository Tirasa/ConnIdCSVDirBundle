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
package net.tirasa.connid.bundles.csvdir.database;

import java.io.File;
import java.io.FileFilter;
import net.tirasa.connid.bundles.csvdir.CSVDirConfiguration;

public class FileSystem {

    private final CSVDirConfiguration conf;

    private final File sourcePath;

    private final FileFilter fileFilter;

    private long highestTimeStamp;

    public FileSystem(final CSVDirConfiguration conf) {
        this.conf = conf;
        this.sourcePath = new File(conf.getSourcePath());
        this.fileFilter = new FileFilter() {

            @Override
            public boolean accept(final File file) {
                return isMatched(file);
            }
        };
    }

    public final File[] getAllCsvFiles() {
        final File[] csvFiles = sourcePath.listFiles(fileFilter);
        return returnNewArrayIfCsvFilesIsEmpty(csvFiles);
    }

    public final File getLastModifiedCsvFile() {
        final File[] csvFiles = getAllCsvFiles();

        long tm = 0L;

        File lastModifiedFile = null;

        for (File file : csvFiles) {
            if (file.lastModified() > tm) {
                tm = file.lastModified();
                lastModifiedFile = file;
            }
        }

        return lastModifiedFile;
    }

    public final File[] getModifiedCsvFiles(final long timeStamp) {
        final File[] csvFiles = sourcePath.listFiles(new FileFilter() {

            @Override
            public boolean accept(final File file) {
                return isMatched(file) && file.lastModified() > timeStamp;
            }
        });

        for (File file : csvFiles) {
            if (file.lastModified() > highestTimeStamp) {
                highestTimeStamp = file.lastModified();
            }
        }

        return returnNewArrayIfCsvFilesIsEmpty(csvFiles);
    }

    private File[] returnNewArrayIfCsvFilesIsEmpty(final File[] csvFiles) {
        return csvFiles == null ? new File[] {} : csvFiles;
    }

    public final long getHighestTimeStamp() {
        return highestTimeStamp;
    }

    private boolean isMatched(final File file) {
        return !file.isDirectory()
                && (file.getName().matches(conf.getFileMask())
                || file.getName().matches(FileToDB.DEFAULT_PREFIX + ".*\\.csv"));
    }
}
