/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ext.base;

import com.powsybl.afs.AfsException;
import com.powsybl.afs.ProjectFile;
import com.powsybl.afs.ProjectFileCreationContext;
import com.powsybl.afs.storage.AppStorageDataSource;
import com.powsybl.commons.datasource.ReadOnlyDataSource;
import com.powsybl.iidm.import_.Importer;
import com.powsybl.iidm.import_.ImportersLoader;
import com.powsybl.iidm.network.Network;
import com.powsybl.iidm.network.NetworkListener;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

/**
 *  A type of {@code ProjectFile} which represents a {@link Network} imported to the project,
 *  and provides methods to get the {@code Network} object or query it with a script.
 *
 *  @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class ImportedCase extends ProjectFile implements ProjectCase {

    public static final String PSEUDO_CLASS = "importedCase";
    public static final int VERSION = 0;

    static final String FORMAT = "format";
    static final String PARAMETERS = "parameters";
    static final String CONTENT_DATA_START = "content__";

    private final ImportersLoader importersLoader;

    public ImportedCase(ProjectFileCreationContext context, ImportersLoader importersLoader) {
        super(context, VERSION);
        this.importersLoader = Objects.requireNonNull(importersLoader);
    }

    public ReadOnlyDataSource getDataSource() {
        String contentDataName = storage.getDataNames(info.getId()).stream()
                .filter(name -> name.startsWith(CONTENT_DATA_START))
                .findFirst()
                .orElse(null);

        if (contentDataName != null) {
            String filename = contentDataName.substring(CONTENT_DATA_START.length(), contentDataName.length());
            CompressedInput input = new CompressedInput() {
                @Override
                public String getName() {
                    return filename;
                }

                @Override
                public InputStream newCompressedInputStream() {
                    return storage.readBinaryData(info.getId(), contentDataName)
                            .orElse(null);
                }

                @Override
                public InputStream newUncompressedInputStream() {
                    return storage.readBinaryData(info.getId(), contentDataName)
                            .map(is -> {
                                try {
                                    return new GZIPInputStream(is);
                                } catch (IOException exc) {
                                    throw new UncheckedIOException(exc);
                                }
                            })
                            .orElse(null);
                }
            };
            return input.asDatasource();
        }
        return new AppStorageDataSource(storage, info.getId(), info.getName());
    }

    static String getContentDataName(String filename) {
        return CONTENT_DATA_START + filename;
    }

    public Properties getParameters() {
        Properties parameters = new Properties();
        try (Reader reader = new InputStreamReader(storage.readBinaryData(info.getId(), PARAMETERS).orElseThrow(AssertionError::new), StandardCharsets.UTF_8)) {
            parameters.load(reader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return parameters;
    }

    public Importer getImporter() {
        String format = info.getGenericMetadata().getString(FORMAT);
        return importersLoader.loadImporters()
                .stream()
                .filter(importer -> importer.getFormat().equals(format))
                .findFirst()
                .orElseThrow(() -> new AfsException("Importer not found for format " + format));
    }

    @Override
    public String queryNetwork(ScriptType scriptType, String scriptContent) {
        Objects.requireNonNull(scriptType);
        Objects.requireNonNull(scriptContent);
        return findService(NetworkCacheService.class).queryNetwork(this, scriptType, scriptContent);
    }

    @Override
    public Network getNetwork() {
        return findService(NetworkCacheService.class).getNetwork(this);
    }

    @Override
    public Network getNetwork(List<NetworkListener> listeners) {
        return findService(NetworkCacheService.class).getNetwork(this, listeners);
    }

    @Override
    public void invalidateNetworkCache() {
        findService(NetworkCacheService.class).invalidateCache(this);
    }

    @Override
    protected void invalidate() {
        super.invalidate();

        invalidateNetworkCache();
    }

    @Override
    public void addListener(ProjectCaseListener l) {
        // nothing to do
    }

    @Override
    public void removeListener(ProjectCaseListener l) {
        // nothing to do
    }
}
