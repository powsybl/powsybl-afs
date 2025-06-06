/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ext.base;

import com.powsybl.afs.AfsException;
import groovy.lang.GroovyRuntimeException;
import org.codehaus.groovy.control.ErrorCollector;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.codehaus.groovy.control.messages.ExceptionMessage;
import org.codehaus.groovy.control.messages.Message;
import org.codehaus.groovy.control.messages.SyntaxErrorMessage;
import org.codehaus.groovy.syntax.SyntaxException;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 *
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class ScriptError implements Serializable {

    @Serial
    private static final long serialVersionUID = 8116688293120382652L;

    private final String message;
    private final int startLine;
    private final int startColumn;
    private final int endLine;
    private final int endColumn;

    public ScriptError(String message, int startLine, int startColumn, int endLine, int endColumn) {
        this.message = Objects.requireNonNull(message);
        this.startLine = startLine;
        this.startColumn = startColumn;
        this.endLine = endLine;
        this.endColumn = endColumn;
    }

    public ScriptError(String message) {
        this.message = Objects.requireNonNull(message);
        this.startLine = -1;
        this.startColumn = -1;
        this.endLine = -1;
        this.endColumn = -1;
    }

    public static ScriptError fromGroovyException(MultipleCompilationErrorsException e) {
        ErrorCollector errorCollector = e.getErrorCollector();
        if (errorCollector.getErrorCount() > 0) {
            Message error = errorCollector.getError(0);
            if (error instanceof SyntaxErrorMessage syntaxErrorMessage) {
                SyntaxException cause = syntaxErrorMessage.getCause();
                return new ScriptError(cause.getMessage(), cause.getStartLine(), cause.getStartColumn(),
                        cause.getEndLine(), cause.getEndColumn());
            } else if (error instanceof ExceptionMessage exceptionMessage) {
                Exception cause = exceptionMessage.getCause();
                return new ScriptError(cause.getMessage());
            } else {
                throw new AfsException("SyntaxErrorMessage or ExceptionMessage is expected");
            }
        } else {
            throw new AfsException("At least one error is expected");
        }
    }

    public static ScriptError fromGroovyException(GroovyRuntimeException e, String scriptName) {
        Objects.requireNonNull(scriptName);
        for (StackTraceElement element : e.getStackTrace()) {
            if (scriptName.equals(element.getFileName())
                    && scriptName.equals(element.getClassName())
                    && "run".equals(element.getMethodName())) {
                return new ScriptError(e.getMessage(), element.getLineNumber(), -1, element.getLineNumber(), -1);
            }
        }
        return null;
    }

    public String getMessage() {
        return message;
    }

    public int getStartLine() {
        return startLine;
    }

    public int getStartColumn() {
        return startColumn;
    }

    public int getEndLine() {
        return endLine;
    }

    public int getEndColumn() {
        return endColumn;
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, startLine, startColumn, endLine, endColumn);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ScriptError other) {
            return message.equals(other.message) &&
                    startLine == other.startLine &&
                    startColumn == other.startColumn &&
                    endLine == other.endLine &&
                    endColumn == other.endColumn;
        }
        return false;
    }

    @Override
    public String toString() {
        return "ScriptError(message=" + message + ", startLine=" + startLine + ", startColumn=" + startColumn +
                ", endLine=" + endLine + ", endColumn=" + endColumn + ")";
    }
}
