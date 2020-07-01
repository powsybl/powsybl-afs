/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.powsybl.commons.datasource.DataSource;
import com.powsybl.commons.datastore.DataStore;

/**
 * A datastore corresponding to a data blob stored in the file system.
 * A data blob is associated to a node and a name identifying it among data blobs of this node.
 *
 * @author Giovanni Ferrari <giovanni.ferrari at techrain.eu>
 */
public class AppStorageDataStore implements DataStore {

    private static final String SEPARATOR = "__";

    public interface Name {

        static Name parse(String text) {
            Objects.requireNonNull(text);
            if (text.startsWith(FileName.START_PATTERN)) {
                String fileName = text.substring(FileName.START_PATTERN.length());
                return new FileName(fileName);
            } else {
                return null;
            }
        }

        static <T> T parse(String text, NameHandler<T> handler) {
            Objects.requireNonNull(handler);
            T result;
            AppStorageDataStore.Name dataSrcName = parse(text);
            try {
                if (dataSrcName instanceof AppStorageDataStore.FileName) {
                    result = handler.onFileName((AppStorageDataStore.FileName) dataSrcName);
                } else {
                    result = handler.onOther(dataSrcName);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return result;
        }
    }

    public interface NameHandler<T> {

        T onFileName(AppStorageDataStore.FileName fileName) throws IOException;

        T onOther(AppStorageDataStore.Name name);
    }

    public static class FileName implements Name {

        static final String START_PATTERN = "DATA_STORE_FILE_NAME" + SEPARATOR;

        private final String name;

        FileName(String name) {
            this.name = Objects.requireNonNull(name);
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return START_PATTERN + name;
        }
    }

    private final AppStorage storage;

    private final String nodeId;

    public AppStorageDataStore(AppStorage storage, String nodeId) {
        this.storage = Objects.requireNonNull(storage);
        this.nodeId = Objects.requireNonNull(nodeId);
    }

    @Override
    public OutputStream newOutputStream(String fileName, boolean append) {
        Objects.requireNonNull(fileName);
        if (append) {
            throw new UnsupportedOperationException("Append mode not supported");
        }
        return storage.writeBinaryData(nodeId, new FileName(fileName).toString());
    }

    @Override
    public boolean exists(String fileName) {
        return storage.dataExists(nodeId, new FileName(fileName).toString());
    }

    @Override
    public InputStream newInputStream(String fileName) throws IOException {
        return storage.readBinaryData(nodeId, new FileName(fileName).toString())
                .orElseThrow(() -> new IOException(fileName + " does not exist"));
    }

    private static final Logger LOG = LoggerFactory.getLogger(AppStorageDataStore.class);

    @Override
    public List<String> getEntryNames() throws IOException {
        List<String> names = storage.getDataNames(nodeId).stream()
                .map(name -> Name.parse(name, new NameHandler<String>() {

                    @Override
                    public String onFileName(FileName fileName) throws IOException {
                        return fileName.getName();
                    }

                    @Override
                    public String onOther(Name otherName) {
                        // Return the original name
                        return name;
                    }
                })).collect(Collectors.toList());
        LOG.info("AppStorageDataStore::getEntryNames()");
        names.forEach(n -> LOG.info("    {}", n));
        return names;
    }

    @Override
    public DataSource toDataSource(String name) {
        return new AppStorageDataSource(storage, nodeId, name);
    }
}
