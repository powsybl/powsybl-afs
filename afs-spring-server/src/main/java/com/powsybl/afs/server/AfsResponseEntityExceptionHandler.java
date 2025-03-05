/*
 * Copyright (c) 2025, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
 */
package com.powsybl.afs.server;

import com.powsybl.afs.AfsException;
import com.powsybl.afs.ext.base.ScriptException;
import com.powsybl.afs.storage.AfsNodeNotFoundException;
import com.powsybl.afs.storage.AfsStorageException;
import com.powsybl.afs.ws.utils.ExceptionDetail;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
@RestControllerAdvice
public class AfsResponseEntityExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler({ScriptException.class})
    public ResponseEntity<Object> handleScriptException(ScriptException e) {
        return new ResponseEntity<>(new ExceptionDetail(e.getClass().getCanonicalName(), e.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @ExceptionHandler({AfsNodeNotFoundException.class})
    public ResponseEntity<Object> handleNodeNotFoundException(AfsNodeNotFoundException e) {
        return new ResponseEntity<>(new ExceptionDetail(e.getClass().getCanonicalName(), e.getMessage()), HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler({AfsStorageException.class})
    public ResponseEntity<Object> handleAfsStorageException(AfsStorageException e) {
        return new ResponseEntity<>(new ExceptionDetail(e.getClass().getCanonicalName(), e.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @ExceptionHandler({AfsException.class})
    public ResponseEntity<Object> handleOtherAfsException(AfsException e) {
        return new ResponseEntity<>(new ExceptionDetail(e.getClass().getCanonicalName(), e.getMessage()), HttpStatus.BAD_REQUEST);
    }
}
