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
package net.tirasa.connid.bundles.csvdir.database;

import java.io.File;
import java.io.FileFilter;
import java.nio.file.Path;
import net.tirasa.connid.bundles.csvdir.CSVDirConfiguration;

public class FileSystem {

    private final CSVDirConfiguration conf;

    private final File sourcePath;

    private final FileFilter fileFilter;

    public FileSystem(final CSVDirConfiguration conf) {
        this.conf = conf;
        this.sourcePath = Path.of(conf.getSourcePath()).toFile();
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

    public File[] getLastestChangedFiles(final long startFrom) {
        return returnNewArrayIfCsvFilesIsEmpty(sourcePath.listFiles(new FileFilter() {

            @Override
            public boolean accept(final File file) {
                return isMatched(file) && file.lastModified() > startFrom;
            }
        }));
    }

    public long getHighestTimeStamp(final long startFrom) {
        long res = 0L;

        for (File file : returnNewArrayIfCsvFilesIsEmpty(sourcePath.listFiles(new FileFilter() {

            @Override
            public boolean accept(final File file) {
                return isMatched(file) && file.lastModified() > startFrom;
            }
        }))) {
            if (file.lastModified() > res) {
                res = file.lastModified();
            }
        }

        return res;
    }

    public long getHighestTimeStamp(final File[] csvFiles) {
        long res = 0L;

        for (File file : csvFiles) {
            if (file.lastModified() > res) {
                res = file.lastModified();
            }
        }

        return res;
    }

    private File[] returnNewArrayIfCsvFilesIsEmpty(final File[] csvFiles) {
        return csvFiles == null ? new File[] {} : csvFiles;
    }

    private boolean isMatched(final File file) {
        return !file.isDirectory()
                && (file.getName().matches(conf.getFileMask())
                || file.getName().matches(FileToDB.DEFAULT_PREFIX + ".*\\.csv"));
    }
}
