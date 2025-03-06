// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.config.logging;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.RECORD_COMPONENT})
public @interface Loggable {}
