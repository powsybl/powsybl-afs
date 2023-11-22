/*
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.powsybl.afs.ext.base;

import com.google.common.io.CharStreams;
import com.powsybl.afs.AfsException;
import com.powsybl.afs.ProjectFileBuildContext;
import com.powsybl.afs.ProjectFileBuilder;
import com.powsybl.afs.ProjectFileCreationContext;
import com.powsybl.afs.ext.base.events.ScriptModified;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * @author Paul Bui-Quang {@literal <paul.buiquang at rte-france.com>}
 */
public class GenericScriptBuilder implements ProjectFileBuilder<GenericScript> {

    private final ProjectFileBuildContext context;

    private String name;

    private ScriptType type;

    private String content;

    public GenericScriptBuilder(ProjectFileBuildContext context) {
        this.context = Objects.requireNonNull(context);
    }

    public GenericScriptBuilder withName(String name) {
        this.name = name;
        return this;
    }

    public GenericScriptBuilder withType(ScriptType type) {
        this.type = type;
        return this;
    }

    public GenericScriptBuilder withContent(String content) {
        this.content = content;
        return this;
    }

    @Override
    public GenericScript build() {
        // check parameters
        if (name == null) {
            throw new AfsException("Name is not set");
        }
        if (type == null) {
            throw new AfsException("Script type is not set");
        }
        if (content == null) {
            throw new AfsException("Content is not set");
        }

        if (context.getStorage().getChildNode(context.getFolderInfo().getId(), name).isPresent()) {
            throw new AfsException("Parent folder already contains a '" + name + "' node");
        }

        // create project file
        NodeInfo info = context.getStorage().createNode(context.getFolderInfo().getId(), name, GenericScript.PSEUDO_CLASS, "", GenericScript.VERSION,
                new NodeGenericMetadata().setString(GenericScript.SCRIPT_TYPE, type.name()));

        // store script
        try (Reader reader = new StringReader(content);
             Writer writer = new OutputStreamWriter(context.getStorage().writeBinaryData(info.getId(), GenericScript.SCRIPT_CONTENT), StandardCharsets.UTF_8)) {
            CharStreams.copy(reader, writer);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        context.getStorage().setConsistent(info.getId());

        context.getStorage().flush();

        GenericScript genericScript = new GenericScript(new ProjectFileCreationContext(info, context.getStorage(), context.getProject()));

        context.getStorage().getEventsBus().pushEvent(new ScriptModified(info.getId(),
                context.getFolderInfo().getId(), genericScript.getPath().toString()), ScriptModified.TYPENAME);

        return genericScript;
    }
}
