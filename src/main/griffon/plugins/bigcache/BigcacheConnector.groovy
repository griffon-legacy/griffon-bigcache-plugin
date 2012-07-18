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
import org.bigcache.config.model.BigCacheConfig
import org.bigcache.config.model.BigCacheManagerConfig
import org.bigcache.config.model.CacheDefaults
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import griffon.core.GriffonApplication
import griffon.util.Environment
import griffon.util.CallableWithArgs

/**
 * @author Andres Almiray
 */
@Singleton
final class BigcacheConnector implements BigcacheProvider {
    private bootstrap

    private static final Logger LOG = LoggerFactory.getLogger(BigcacheConnector)

    Object withBigcache(String bigcacheManagerName = 'default', Closure closure) {
        BigcacheManagerHolder.instance.withBigcache(bigcacheManagerName, closure)
    }

    public <T> T withBigcache(String bigcacheManagerName = 'default', CallableWithArgs<T> callable) {
        return BigcacheManagerHolder.instance.withBigcache(bigcacheManagerName, callable)
    }

    // ======================================================

    ConfigObject createConfig(GriffonApplication app) {
        def bigcacheManagerClass = app.class.classLoader.loadClass('BigcacheConfig')
        new ConfigSlurper(Environment.current.name).parse(bigcacheManagerClass)
    }

    private ConfigObject narrowConfig(ConfigObject config, String bigcacheManagerName) {
        return bigcacheManagerName == 'default' ? config.manager : config.managers[bigcacheManagerName]
    }

    BigCacheManager connect(GriffonApplication app, ConfigObject config, String bigcacheManagerName = 'default') {
        if (BigcacheManagerHolder.instance.isManagerConnected(bigcacheManagerName)) {
            return BigcacheManagerHolder.instance.getManager(bigcacheManagerName)
        }

        config = narrowConfig(config, bigcacheManagerName)
        app.event('BigcacheConnectStart', [config, bigcacheManagerName])
        BigCacheManager bigcacheManager = startBigcache(bigcacheManagerName, config)
        BigcacheManagerHolder.instance.setManager(bigcacheManagerName, bigcacheManager)
        bootstrap = app.class.classLoader.loadClass('BootstrapBigcache').newInstance()
        bootstrap.metaClass.app = app
        bootstrap.init(bigcacheManagerName, bigcacheManager)
        app.event('BigcacheConnectEnd', [bigcacheManagerName, bigcacheManager])
        bigcacheManager
    }

    void disconnect(GriffonApplication app, ConfigObject config, String bigcacheManagerName = 'default') {
        if (BigcacheManagerHolder.instance.isManagerConnected(bigcacheManagerName)) {
            config = narrowConfig(config, bigcacheManagerName)
            BigCacheManager bigcacheManager = BigcacheManagerHolder.instance.getManager(bigcacheManagerName)
            app.event('BigcacheDisconnectStart', [config, bigcacheManagerName, bigcacheManager])
            bootstrap.destroy(bigcacheManagerName, bigcacheManager)
            stopBigcache(config, bigcacheManager)
            app.event('BigcacheDisconnectEnd', [config, bigcacheManagerName])
            BigcacheManagerHolder.instance.disconnectManager(bigcacheManagerName)
        }
    }

    private BigCacheManager startBigcache(String bigcacheManagerName, ConfigObject config) {
        BigCacheManagerConfig bigCacheManagerConfig = new BigCacheManagerConfig()
        CacheDefaults cacheDefaults = new CacheDefaults()

        config.defaults.each { k, v -> cacheDefaults[k] = v }
        bigCacheManagerConfig.defaults = cacheDefaults
        config.caches.each { name, props ->
            BigCacheConfig cacheConfig = new BigCacheConfig()
            cacheConfig.name = name
            if (props.capacity) cacheConfig.capacityStr = props.capacity
            if (props.maxFragmentation) cacheConfig.maxFragmentation = props.maxFragmentation
            bigCacheManagerConfig.addCacheConfig(cacheConfig)
        }

        BigCacheManager.configure(bigCacheManagerConfig)
    }

    private void stopBigcache(ConfigObject config, BigCacheManager bigcacheManager) {

    }
}
