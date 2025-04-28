// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark a configuration field as loggable. So that its value will be logged on server startup. Any
 * property without this annotation will still be logged but with its value masked. This is internationally this way
 * round so the default is for a property to be considered sensitive and have its value masked. With the aim to avoid
 * accidentally logging sensitive information.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.RECORD_COMPONENT})
public @interface Loggable {}
