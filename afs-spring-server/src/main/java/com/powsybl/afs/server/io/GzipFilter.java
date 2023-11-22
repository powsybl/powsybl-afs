/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.server.io;

import org.springframework.http.HttpHeaders;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * An http servlet filter which tries to handle automatic gzip compression and decompression:
 * <ul>
 *     <li>for POST request with gzip content-encoding, wrap the request to gunzip its content</li>
 *     <li>Wraps responses : responses with gzip content-encoding will get their output stream wrapped in a gzip output stream</li>
 * </ul>
 *
 * The wrapping of response does not work very well:
 * the gzip output stream needs to be closed, but apparently spring never closes it.
 * Therefore, this filter closes the stream after the filter chain, but it will break
 * the streaming of responses ({@link org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody}.
 * This filter must not be activated on such endpoints.
 *
 * @author Sylvain Leclerc {@literal <sylvain.leclerc at rte-france.com>}
 */
public class GzipFilter implements Filter {

    @Override
    public final void doFilter(final ServletRequest servletRequest,
                               final ServletResponse servletResponse,
                               final FilterChain chain) throws IOException, ServletException {

        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        String requestedContentEncoding = request.getHeader(HttpHeaders.CONTENT_ENCODING);
        boolean isGzipped = requestedContentEncoding != null && requestedContentEncoding.contains("gzip");

        boolean requestTypeSupported = "POST".equals(request.getMethod()) || "PUT".equals(request.getMethod());
        if (isGzipped && !requestTypeSupported) {
            throw new IllegalStateException(request.getMethod()
                    + " is not supports gzipped body of parameters."
                    + " Only POST requests are currently supported.");
        }
        if (isGzipped) {
            request = new GzippedRequestWrapper((HttpServletRequest) servletRequest);
        }
        GzippedResponseWrapper gzipResponse = new GzippedResponseWrapper(response);
        chain.doFilter(request, gzipResponse);

        //This seems necessary because otherwise spring does not close the output stream,
        //and therefore the gzip data is invalid.
        applyAfterCompletion(request, gzipResponse::finish);
    }

    @FunctionalInterface
    private interface IORunnable {
        void run() throws IOException;
    }

    /**
     * Apply a method after the completion of the request, be it synchronous or asynchronous.
     */
    private void applyAfterCompletion(HttpServletRequest request, IORunnable runnable) throws IOException {
        if (request.isAsyncStarted()) {
            request.getAsyncContext().addListener(new AsyncListener() {
                @Override
                public void onComplete(AsyncEvent asyncEvent) throws IOException {
                    runnable.run();
                }

                @Override
                public void onTimeout(AsyncEvent asyncEvent) throws IOException {
                    runnable.run();
                }

                @Override
                public void onError(AsyncEvent asyncEvent) throws IOException {
                    runnable.run();
                }

                @Override
                public void onStartAsync(AsyncEvent asyncEvent) throws IOException {
                }
            });
        } else {
            runnable.run();
        }
    }

    @Override
    public void init(FilterConfig filterConfig) {
    }

    @Override
    public void destroy() {
    }
}
