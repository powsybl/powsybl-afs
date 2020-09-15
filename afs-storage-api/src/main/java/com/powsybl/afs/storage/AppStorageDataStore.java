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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.powsybl.commons.datastore.DataStore;

/**
 * A datastore corresponding to a data blob stored in the file system.
 * A data blob is associated to a node and a name identifying it among data blobs of this node.
 *
 * @author Giovanni Ferrari <giovanni.ferrari at techrain.eu>
 */
public class AppStorageDataStore implements DataStore {

    private static final String SEPARATOR = "__";

    static final String START_PATTERN = "DATA_STORE_ENTRY_NAME" + SEPARATOR;

    private final AppStorage storage;

    private final String nodeId;

    public AppStorageDataStore(AppStorage storage, String nodeId) {
        this.storage = Objects.requireNonNull(storage);
        this.nodeId = Objects.requireNonNull(nodeId);
    }

    public static String getEntryDataName(String entryName) {
        return START_PATTERN.concat(entryName);
    }

    @Override
    public OutputStream newOutputStream(String fileName, boolean append) {
        Objects.requireNonNull(fileName);
        if (append) {
            throw new UnsupportedOperationException("Append mode not supported");
        }
        return storage.writeBinaryData(nodeId, getEntryDataName(fileName));
    }

    @Override
    public boolean exists(String fileName) {
        return storage.dataExists(nodeId, getEntryDataName(fileName));
    }

    @Override
    public InputStream newInputStream(String fileName) throws IOException {
        return storage.readBinaryData(nodeId, getEntryDataName(fileName))
                .orElseThrow(() -> new IOException(fileName + " does not exist"));
    }

    private static final Logger LOG = LoggerFactory.getLogger(AppStorageDataStore.class);

    @Override
    public List<String> getEntryNames() throws IOException {
        List<String> names = storage.getDataNames(nodeId).stream()
                .map(name -> getEntryDataName(name)).collect(Collectors.toList());
        LOG.info("AppStorageDataStore::getEntryNames()");
        names.forEach(n -> LOG.info("    {}", n));
        return names;
    }

}
