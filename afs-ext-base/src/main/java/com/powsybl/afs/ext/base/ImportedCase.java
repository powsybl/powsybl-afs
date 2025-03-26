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
import com.powsybl.iidm.network.Importer;
import com.powsybl.iidm.network.ImportersLoader;
import com.powsybl.iidm.network.Network;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 *  A type of {@code ProjectFile} which represents a {@link Network} imported to the project,
 *  and provides methods to get the {@code Network} object or query it with a script.
 *
 *  @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class ImportedCase extends AbstractProjectCase {

    public static final String PSEUDO_CLASS = "importedCase";
    public static final int VERSION = 0;

    static final String FORMAT = "format";
    static final String PARAMETERS = "parameters";

    private final ImportersLoader importersLoader;

    public ImportedCase(ProjectFileCreationContext context, ImportersLoader importersLoader) {
        super(context, VERSION);
        this.importersLoader = Objects.requireNonNull(importersLoader);
    }

    public ReadOnlyDataSource getDataSource() {
        return new AppStorageDataSource(storage, info.getId(), info.getName());
    }

    public Properties getParameters() {
        Properties parameters = new Properties();
        try (Reader reader = new InputStreamReader(storage.readBinaryData(info.getId(), PARAMETERS)
            .orElseThrow(() -> new AfsException("Unable to read data from node " + info.getId())), StandardCharsets.UTF_8)) {
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
    protected List<ProjectFile> invalidate() {
        List<ProjectFile> dependencies = super.invalidate();

        invalidateNetworkCache();

        return dependencies;
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
