/*
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.powsybl.afs.ext.base;

import com.google.auto.service.AutoService;
import com.powsybl.afs.ProjectFileBuildContext;
import com.powsybl.afs.ProjectFileCreationContext;
import com.powsybl.afs.ProjectFileExtension;

/**
 * @author Paul Bui-Quang <paul.buiquang at rte-france.com>
 */
@AutoService(ProjectFileExtension.class)
public class GenericScriptExtension implements ProjectFileExtension<GenericScript, GenericScriptBuilder> {
    @Override
    public Class<GenericScript> getProjectFileClass() {
        return GenericScript.class;
    }

    @Override
    public String getProjectFilePseudoClass() {
        return GenericScript.PSEUDO_CLASS;
    }

    @Override
    public Class<GenericScriptBuilder> getProjectFileBuilderClass() {
        return GenericScriptBuilder.class;
    }

    @Override
    public GenericScript createProjectFile(ProjectFileCreationContext context) {
        return new GenericScript(context);
    }

    @Override
    public GenericScriptBuilder createProjectFileBuilder(ProjectFileBuildContext context) {
        return new GenericScriptBuilder(context);
    }
}
