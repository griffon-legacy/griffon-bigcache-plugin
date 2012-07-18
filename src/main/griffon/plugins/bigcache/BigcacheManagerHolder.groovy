/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package griffon.plugins.bigcache

import org.bigcache.BigCacheManager
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import griffon.core.GriffonApplication
import griffon.util.CallableWithArgs
import static griffon.util.GriffonNameUtils.isBlank

/**
 * @author Andres Almiray
 */
@Singleton
class BigcacheManagerHolder implements BigcacheProvider {
    private static final Logger LOG = LoggerFactory.getLogger(BigcacheManagerHolder)
    private static final Object[] LOCK = new Object[0]
    private final Map<String, BigCacheManager> managers = [:]

    String[] getManagerNames() {
        List<String> bigcacheManagerNames = [].addAll(managers.keySet())
        bigcacheManagerNames.toArray(new String[bigcacheManagerNames.size()])
    }

    BigCacheManager getManager(String bigcacheManagerName = 'default') {
        if (isBlank(bigcacheManagerName)) bigcacheManagerName = 'default'
        retrieveBigcacheManager(bigcacheManagerName)
    }

    void setManager(String bigcacheManagerName = 'default', BigCacheManager bcm) {
        if (isBlank(bigcacheManagerName)) bigcacheManagerName = 'default'
        storeBigcacheManager(bigcacheManagerName, bcm)
    }

    Object withBigcache(String bigcacheManagerName = 'default', Closure closure) {
        BigCacheManager manager = fetchManager(bigcacheManagerName)
        if (LOG.debugEnabled) LOG.debug("Executing statement on manager '$bigcacheManagerName'")
        return closure(bigcacheManagerName, manager)
    }

    public <T> T withBigcache(String bigcacheManagerName = 'default', CallableWithArgs<T> callable) {
        BigCacheManager manager = fetchManager(bigcacheManagerName)
        if (LOG.debugEnabled) LOG.debug("Executing statement on manager '$bigcacheManagerName'")
        callable.args = [bigcacheManagerName, manager] as Object[]
        return callable.call()
    }

    boolean isManagerConnected(String bigcacheManagerName) {
        if (isBlank(bigcacheManagerName)) bigcacheManagerName = 'default'
        retrieveBigcacheManager(bigcacheManagerName) != null
    }

    void disconnectManager(String bigcacheManagerName) {
        if (isBlank(bigcacheManagerName)) bigcacheManagerName = 'default'
        storeBigcacheManager(bigcacheManagerName, null)
    }

    private BigCacheManager fetchManager(String bigcacheManagerName) {
        if (isBlank(bigcacheManagerName)) bigcacheManagerName = 'default'
        BigCacheManager manager = retrieveBigcacheManager(bigcacheManagerName)
        if (manager == null) {
            GriffonApplication app = ApplicationHolder.application
            ConfigObject config = BigcacheConnector.instance.createConfig(app)
            manager = BigcacheConnector.instance.connect(app, config, bigcacheManagerName)
        }

        if (manager == null) {
            throw new IllegalArgumentException("No such bigcache manager configuration for name $bigcacheManagerName")
        }
        manager
    }

    private BigCacheManager retrieveBigcacheManager(String bigcacheManagerName) {
        synchronized (LOCK) {
            managers[bigcacheManagerName]
        }
    }

    private void storeBigcacheManager(String bigcacheManagerName, BigCacheManager bcm) {
        synchronized (LOCK) {
            managers[bigcacheManagerName] = bcm
        }
    }
}
