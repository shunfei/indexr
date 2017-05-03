package io.indexr.plugin;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * You should declare your plugin in the "io.indexr.plugin" package, and then mark the init method
 * with @InitPlugin.
 *
 * example:
 * <pre>
 * <code>package io.indexr.plugin;
 * public class PluginTest {
 *  {@literal @}InitPlugin
 *   public static void init() {
 *     System.out.println("Hello!");
 *     // Do something initialization.
 *   }
 * }
 * </code>
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@interface InitPlugin {}
