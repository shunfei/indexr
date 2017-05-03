package io.indexr.plugin;

import com.google.common.reflect.ClassPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Set;

public class Plugins {
    private static final Logger logger = LoggerFactory.getLogger(Plugins.class);
    private static volatile boolean loaded = false;

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
            ClassPath classPath = ClassPath.from(Plugins.class.getClassLoader());
            Set<ClassPath.ClassInfo> classes = classPath.getTopLevelClasses(Plugins.class.getPackage().getName());
            for (ClassPath.ClassInfo ci : classes) {
                Method[] methods = ci.load().getDeclaredMethods();
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
                            logger.info("Start run plugin method: {}#{}", ci.getSimpleName(), m.getName());
                            m.invoke(null);
                            sucOne = true;
                            logger.info("Finish run plugin method: {}#{}", ci.getSimpleName(), m.getName());
                        } catch (Exception e) {
                            logger.error("Plugin method failed: {}#{} ", ci.getSimpleName(), m.getName(), e);
                        }
                    }
                }
            }
            return sucOne;
        }
    }
}
