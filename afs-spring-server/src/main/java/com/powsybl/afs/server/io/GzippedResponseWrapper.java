/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.server.io;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;

import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;

import org.springframework.http.HttpHeaders;

/**
 * Wraps a response to gzip its content if its content-encoding is set to "gzip".
 *
 * @author Sylvain Leclerc {@literal <sylvain.leclerc at rte-france.com>}
 */
public class GzippedResponseWrapper extends HttpServletResponseWrapper {

    private ServletOutputStream servletOuput;
    private PrintWriter printerWriter;

    public GzippedResponseWrapper(HttpServletResponse response) {
        super(response);
    }

    /**
     * Closes the underlying output streams / writers.
     * This is necessary to get a valid gzipped body.
     */
    public void finish() throws IOException {
        if (printerWriter != null) {
            printerWriter.close();
        }
        if (servletOuput != null) {
            servletOuput.close();
        }
    }

    @Override
    public void flushBuffer() throws IOException {
        if (printerWriter != null) {
            printerWriter.flush();
        }
        if (servletOuput != null) {
            servletOuput.flush();
        }
        super.flushBuffer();
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
        if (servletOuput == null) {
            if (super.getHeader(HttpHeaders.CONTENT_ENCODING) != null && super.getHeader(HttpHeaders.CONTENT_ENCODING).equals("gzip")) {
                servletOuput = new GzipResponseStream(getResponse().getOutputStream());
            } else {
                servletOuput = getResponse().getOutputStream();
            }
        }
        return servletOuput;
    }

    @Override
    public PrintWriter getWriter() throws IOException {
        if (printerWriter == null) {
            printerWriter = new PrintWriter(new OutputStreamWriter(getOutputStream()));
        }
        return printerWriter;
    }

    static class GzipResponseStream extends ServletOutputStream {

        private final ServletOutputStream delegate;
        private final GZIPOutputStream gzOut;

        public GzipResponseStream(ServletOutputStream delegate) throws IOException {
            this.delegate = Objects.requireNonNull(delegate);
            this.gzOut = new GZIPOutputStream(delegate);
        }

        @Override
        public boolean isReady() {
            return delegate.isReady();
        }

        @Override
        public void setWriteListener(WriteListener listener) {
            delegate.setWriteListener(listener);
        }

        @Override
        public void flush() throws IOException {
            gzOut.flush();
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            gzOut.close();
            delegate.close();
        }

        @Override
        public void write(int b) throws IOException {
            gzOut.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            gzOut.write(b, off, len);
        }

        @Override
        public void write(byte[] b) throws IOException {
            gzOut.write(b);
        }
    }
}
