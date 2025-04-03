/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ext.base;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.powsybl.afs.ProjectFile;
import com.powsybl.commons.util.WeakListenerList;
import com.powsybl.scripting.groovy.GroovyScriptExtension;
import org.apache.commons.lang3.function.TriFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
public class ScriptCache<F extends ProjectFile, V, L> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScriptCache.class);

    private final Cache<String, ScriptResult<V>> cache;

    private final Map<String, WeakListenerList<L>> listeners = new ConcurrentHashMap<>();

    private final TriFunction<F, Iterable<GroovyScriptExtension>, Map<Class<?>, Object>, ScriptResult<V>> loader;

    public ScriptCache(int maximumSize, int hoursExpiration, TriFunction<F, Iterable<GroovyScriptExtension>, Map<Class<?>, Object>, ScriptResult<V>> loader,
                       BiConsumer<ScriptResult<V>, List<L>> notifier) {
        this.loader = Objects.requireNonNull(loader);
        Objects.requireNonNull(notifier);
        cache = CacheBuilder.newBuilder()
            .maximumSize(maximumSize)
            .expireAfterAccess(hoursExpiration, TimeUnit.HOURS)
            .removalListener(notification -> {
                String projectFileId = (String) notification.getKey();

                LOGGER.info("Project file {} cache removed ({})", projectFileId, notification.getCause());

                // notification
                notifier.accept((ScriptResult<V>) notification.getValue(), getListeners(projectFileId).toList());
            })
            .build();
    }

    public ScriptResult<V> get(F projectFile) {
        return get(projectFile, Collections.emptyList(), Collections.emptyMap());
    }

    public ScriptResult<V> get(F projectFile, Iterable<GroovyScriptExtension> extensions, Map<Class<?>, Object> contextObjects) {
        Objects.requireNonNull(projectFile);
        try {
            return cache.get(projectFile.getId(), () -> loader.apply(projectFile, extensions, contextObjects));
        } catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    public void invalidate(F projectFile) {
        Objects.requireNonNull(projectFile);
        cache.invalidate(projectFile.getId());
    }

    private WeakListenerList<L> getListeners(String projectFileId) {
        return listeners.computeIfAbsent(projectFileId, s -> new WeakListenerList<>());
    }

    public void addListener(F projectFile, L listener) {
        Objects.requireNonNull(projectFile);
        Objects.requireNonNull(listener);
        getListeners(projectFile.getId()).add(listener);
    }

    public void removeListener(F projectFile, L listener) {
        Objects.requireNonNull(projectFile);
        Objects.requireNonNull(listener);
        getListeners(projectFile.getId()).remove(listener);
    }
}
