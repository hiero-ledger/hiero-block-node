/*
 * Copyright (C) 2024 Hedera Hashgraph, LLC
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

package com.hedera.block.common.constants;

import edu.umd.cs.findbugs.annotations.NonNull;

/** A class that holds common error message {@link String} literals that are used across projects */
public final class ErrorMessageConstants {
    @NonNull
    public static final String CREATING_INSTANCES_NOT_SUPPORTED =
            "Creating instances is not supported!";

    private ErrorMessageConstants() {
        throw new UnsupportedOperationException(CREATING_INSTANCES_NOT_SUPPORTED);
    }
}
