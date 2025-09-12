/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.server.events;

import com.powsybl.afs.server.AppDataWrapper;
import com.powsybl.afs.server.StorageServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.HandshakeInterceptor;

import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import java.util.Objects;

@Configuration
@EnableWebSocket
public class WebSocketServer implements WebSocketConfigurer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketServer.class);

    private final AppDataWrapper appDataWrapper;
    private final WebSocketContext webSocketContext;

    @Autowired
    public WebSocketServer(AppDataWrapper appDataWrapper,
                           WebSocketContext webSocketContext) {
        this.appDataWrapper = Objects.requireNonNull(appDataWrapper);
        this.webSocketContext = Objects.requireNonNull(webSocketContext);
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry
            .addHandler(new NodeEventHandler(appDataWrapper, webSocketContext), "/messages/afs/" + StorageServer.API_VERSION + "/node_events/{fileSystemName}")
            .addHandler(new TaskEventHandler(appDataWrapper, webSocketContext), "/messages/afs/" + StorageServer.API_VERSION + "/task_events/{fileSystemName}/{projectId}")
            .setAllowedOrigins("*")
            .addInterceptors(new UriTemplateHandshakeInterceptor());
    }

    private static final class UriTemplateHandshakeInterceptor implements HandshakeInterceptor {
        @Override
        public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler, Map<String, Object> attributes) throws Exception {
            HttpServletRequest origRequest = ((ServletServerHttpRequest) request).getServletRequest();
            /* Retrieve template variables */
            Map<String, String> uriTemplateVars = (Map<String, String>) origRequest.getAttribute(HandlerMapping.URI_TEMPLATE_VARIABLES_ATTRIBUTE);
            /* Put template variables into WebSocket session attributes */
            if (uriTemplateVars != null) {
                attributes.putAll(uriTemplateVars);
            }
            return true;
        }

        @Override
        public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler, Exception exception) {
        }
    }
}
