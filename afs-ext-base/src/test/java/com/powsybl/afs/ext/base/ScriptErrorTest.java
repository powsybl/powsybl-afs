/*
 * Copyright (c) 2025, RTE (https://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 * SPDX-License-Identifier: MPL-2.0
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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Nicolas Rol {@literal <nicolas.rol at rte-france.com>}
 */
class ScriptErrorTest {

    @Test
    void fullConstructorTest() {
        ScriptError error = new ScriptError("error message", 1, 2, 3, 4);
        assertEquals("error message", error.getMessage());
        assertEquals(1, error.getStartLine());
        assertEquals(2, error.getStartColumn());
        assertEquals(3, error.getEndLine());
        assertEquals(4, error.getEndColumn());
    }

    @Test
    void minimalConstructorTest() {
        ScriptError error = new ScriptError("error message");
        assertEquals("error message", error.getMessage());
        assertEquals(-1, error.getStartLine());
        assertEquals(-1, error.getStartColumn());
        assertEquals(-1, error.getEndLine());
        assertEquals(-1, error.getEndColumn());
    }

    @Test
    void toStringTest() {
        ScriptError error = new ScriptError("error message", 1, 2, 3, 4);
        assertEquals("ScriptError(message=error message, startLine=1, startColumn=2, endLine=3, endColumn=4)", error.toString());
    }

    @Test
    void fromGroovyExceptionMultipleCompilationErrorsExceptionSyntaxErrorMessageTest() {
        // Mock the groovy exception
        MultipleCompilationErrorsException exception = mock(MultipleCompilationErrorsException.class);
        ErrorCollector errorCollector = mock(ErrorCollector.class);
        SyntaxErrorMessage syntaxErrorMessage = mock(SyntaxErrorMessage.class);
        SyntaxException cause = mock(SyntaxException.class);
        when(exception.getErrorCollector()).thenReturn(errorCollector);
        when(errorCollector.getErrorCount()).thenReturn(1);
        when(errorCollector.getError(0)).thenReturn(syntaxErrorMessage);
        when(syntaxErrorMessage.getCause()).thenReturn(cause);
        when(cause.getMessage()).thenReturn("error message");
        when(cause.getStartLine()).thenReturn(1);
        when(cause.getStartColumn()).thenReturn(2);
        when(cause.getEndLine()).thenReturn(3);
        when(cause.getEndColumn()).thenReturn(4);

        // Create the ScriptError
        ScriptError error = ScriptError.fromGroovyException(exception);

        // Checks
        assertEquals("error message", error.getMessage());
        assertEquals(1, error.getStartLine());
        assertEquals(2, error.getStartColumn());
        assertEquals(3, error.getEndLine());
        assertEquals(4, error.getEndColumn());
    }

    @Test
    void fromGroovyExceptionMultipleCompilationErrorsExceptionExceptionMessageTest() {
        // Mock the groovy exception
        MultipleCompilationErrorsException exception = mock(MultipleCompilationErrorsException.class);
        ErrorCollector errorCollector = mock(ErrorCollector.class);
        ExceptionMessage exceptionMessage = mock(ExceptionMessage.class);
        Exception cause = mock(Exception.class);
        when(exception.getErrorCollector()).thenReturn(errorCollector);
        when(errorCollector.getErrorCount()).thenReturn(1);
        when(errorCollector.getError(0)).thenReturn(exceptionMessage);
        when(exceptionMessage.getCause()).thenReturn(cause);
        when(cause.getMessage()).thenReturn("error message");

        // Create the ScriptError
        ScriptError error = ScriptError.fromGroovyException(exception);

        // Checks
        assertEquals("error message", error.getMessage());
        assertEquals(-1, error.getStartLine());
        assertEquals(-1, error.getStartColumn());
        assertEquals(-1, error.getEndLine());
        assertEquals(-1, error.getEndColumn());
    }

    @Test
    void fromGroovyExceptionMultipleCompilationErrorsExceptionUnexpectedMessageTypeTest() {
        // Mock the groovy exception
        MultipleCompilationErrorsException exception = mock(MultipleCompilationErrorsException.class);
        ErrorCollector errorCollector = mock(ErrorCollector.class);
        Message errorMessage = mock(Message.class);
        when(exception.getErrorCollector()).thenReturn(errorCollector);
        when(errorCollector.getErrorCount()).thenReturn(1);
        when(errorCollector.getError(0)).thenReturn(errorMessage);

        // Create the ScriptError
        AfsException exceptionThrown = assertThrows(AfsException.class, () -> ScriptError.fromGroovyException(exception));
        assertEquals("SyntaxErrorMessage or ExceptionMessage is expected", exceptionThrown.getMessage());
    }

    @Test
    void fromGroovyExceptionMultipleCompilationErrorsNoErrorExceptionTest() {
        // Mock the groovy exception
        MultipleCompilationErrorsException exception = mock(MultipleCompilationErrorsException.class);
        ErrorCollector errorCollector = mock(ErrorCollector.class);
        when(exception.getErrorCollector()).thenReturn(errorCollector);
        when(errorCollector.getErrorCount()).thenReturn(0);

        // Create the ScriptError
        AfsException exceptionThrown = assertThrows(AfsException.class, () -> ScriptError.fromGroovyException(exception));
        assertEquals("At least one error is expected", exceptionThrown.getMessage());
    }

    @Test
    void fromGroovyExceptionGroovyRuntimeExceptionTest() {
        // Mock the groovy exception
        GroovyRuntimeException exception = mock(GroovyRuntimeException.class);
        StackTraceElement element = mock(StackTraceElement.class);
        StackTraceElement[] stackTraceElements = {element};
        when(exception.getMessage()).thenReturn("exception message");
        when(exception.getStackTrace()).thenReturn(stackTraceElements);
        when(element.getFileName()).thenReturn("test");
        when(element.getClassName()).thenReturn("test");
        when(element.getMethodName()).thenReturn("run");
        when(element.getLineNumber()).thenReturn(1);

        // Create the ScriptError
        ScriptError error = ScriptError.fromGroovyException(exception, "test");

        // Checks
        assertNotNull(error);
        assertEquals("exception message", error.getMessage());
        assertEquals(1, error.getStartLine());
        assertEquals(-1, error.getStartColumn());
        assertEquals(1, error.getEndLine());
        assertEquals(-1, error.getEndColumn());
    }

    @Test
    void fromGroovyExceptionGroovyRuntimeExceptionIncoherentFileNameTest() {
        // Mock the groovy exception
        GroovyRuntimeException exception = mock(GroovyRuntimeException.class);
        StackTraceElement element = mock(StackTraceElement.class);
        StackTraceElement[] stackTraceElements = {element};
        when(exception.getStackTrace()).thenReturn(stackTraceElements);
        when(element.getFileName()).thenReturn("unexpected");

        // Create the ScriptError
        ScriptError error = ScriptError.fromGroovyException(exception, "test");

        // Checks
        assertNull(error);
    }

    @Test
    void fromGroovyExceptionGroovyRuntimeExceptionIncoherentClassNameTest() {
        // Mock the groovy exception
        GroovyRuntimeException exception = mock(GroovyRuntimeException.class);
        StackTraceElement element = mock(StackTraceElement.class);
        StackTraceElement[] stackTraceElements = {element};
        when(exception.getStackTrace()).thenReturn(stackTraceElements);
        when(element.getFileName()).thenReturn("test");
        when(element.getClassName()).thenReturn("unexpected");

        // Create the ScriptError
        ScriptError error = ScriptError.fromGroovyException(exception, "test");

        // Checks
        assertNull(error);
    }

    @Test
    void fromGroovyExceptionGroovyRuntimeExceptionIncoherentMethodNameTest() {
        // Mock the groovy exception
        GroovyRuntimeException exception = mock(GroovyRuntimeException.class);
        StackTraceElement element = mock(StackTraceElement.class);
        StackTraceElement[] stackTraceElements = {element};
        when(exception.getStackTrace()).thenReturn(stackTraceElements);
        when(element.getFileName()).thenReturn("test");
        when(element.getClassName()).thenReturn("test");
        when(element.getMethodName()).thenReturn("unexpected");

        // Create the ScriptError
        ScriptError error = ScriptError.fromGroovyException(exception, "test");

        // Checks
        assertNull(error);
    }
}
