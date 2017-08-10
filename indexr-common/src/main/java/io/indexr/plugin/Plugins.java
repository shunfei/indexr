package io.indexr.plugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class Plugins {
    private static final Logger logger = LoggerFactory.getLogger(Plugins.class);
    private static volatile boolean loaded = false;
    private static String[] pluginClasses = {"io.indexr.plugin.VLTPlugin"};

    public static boolean loadPluginsNEX() {
        try {
            return loadPlugins();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean loadPlugins() throws Exception {
        synchronized (Plugins.class) {
            if (loaded) {
                return true;
            }
            loaded = true;

            boolean sucOne = false;
            for (String cn : pluginClasses) {
                Class<?> clzz = Class.forName(cn);
                Method[] methods = clzz.getDeclaredMethods();
                for (Method m : methods) {
                    int mod = m.getModifiers();
                    if (!Modifier.isStatic(mod) && m.getParameterCount() > 0) {
                        continue;
                    }
                    boolean ok = false;
                    for (Annotation ann : m.getAnnotations()) {
                        if (ann instanceof InitPlugin) {
                            ok = true;
                            break;
                        }
                    }
                    if (ok) {
                        try {
                            logger.info("Start run plugin method: {}#{}", cn, m.getName());
                            m.invoke(null);
                            sucOne = true;
                            logger.info("Finish run plugin method: {}#{}", cn, m.getName());
                        } catch (Exception e) {
                            logger.error("Plugin method failed: {}#{} ", cn, m.getName(), e);
                        }
                    }
                }
            }
            return sucOne;
        }
    }
}
