/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.server.io;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

/**
 * Wraps a request to gunzip its content.
 */
public class GzippedRequestWrapper extends HttpServletRequestWrapper {

    private ServletInputStream in;

    public GzippedRequestWrapper(final HttpServletRequest request) {
        super(request);
    }

    @Override
    public ServletInputStream getInputStream() throws IOException {
        if (in == null) {
            in = new GzippedServletInputStream(getRequest().getInputStream());
        }
        return in;
    }

    private static final class GzippedServletInputStream extends ServletInputStream {

        private final ServletInputStream delegate;
        private final GZIPInputStream gzipIn;

        private GzippedServletInputStream(ServletInputStream delegate) throws IOException {
            this.delegate = delegate;
            this.gzipIn = new GZIPInputStream(delegate);
        }

        @Override
        public int read() throws IOException {
            return gzipIn.read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            return gzipIn.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return gzipIn.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
            gzipIn.close();
        }

        @Override
        public boolean isFinished() {
            return delegate.isFinished();
        }

        @Override
        public boolean isReady() {
            return delegate.isReady();
        }

        @Override
        public void setReadListener(ReadListener listener) {
            delegate.setReadListener(listener);
        }
    }
}
