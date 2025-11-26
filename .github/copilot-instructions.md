# Project-specific Copilot instructions

## How to build and run

- when working with gradle module `:tools-and-tests:tools` it is actually `:tools`. Default full build task is
  `shadowJar` which builds all modules and creates an "all-in-one" jar `tools-and-tests/tools/build/libs/tools-0.21.0-SNAPSHOT-all.jar`.
- to run the block CLI tool `:tools` after building, use: `java -jar  tools-and-tests/tools/build/libs/tools-0.21.0-SNAPSHOT-all.jar [args]`

## Coding style and conventions

This provides explicit, actionable guidance for generating Java source that matches this repository's coding style and formatting rules.

- File header and license:
  - Every Java source file must start with the SPDX license header exactly as a single-line comment:
    // SPDX-License-Identifier: Apache-2.0
  - The SPDX header must be the first non-empty line in the file, followed by the `package` statement.
- Package and imports:
  - Always include a `package` declaration after the SPDX header.
  - Do not use wildcard imports (`import java.util.*`).
  - Preferred import ordering: java.*, javax.*, org.*, com.*, then project packages (alphabetical within each group).
  - Keep a single blank line between the package statement and imports, and a single blank line between imports and type declaration.
- Javadoc:
  - Provide Javadoc for all public classes, interfaces, enums, methods, and fields.
  - For short descriptions prefer a single-line Javadoc, e.g.:
    /** The Constants class defines the constants for the block simulator. */
  - For longer descriptions, use a standard block Javadoc and avoid blank lines without <p> tags if a paragraph split is required, e.g.:
    /**
    * Builds X and returns Y.
      *
      *

      This method does ...

    *
    * @param input the input value
    * @return the result
    */

  - Include `@param`, `@return`, and `@throws` tags where applicable for public APIs.

  - Close all HTML tags and use `<ul>`, `<li>` for lists (no Markdown lists inside Javadoc).

- Naming, modifiers, and fields:

  - Use long descriptive variable names rather than single-letter names (except loop indices i, j, k).
  - Prefer explicit types for public method signatures and fields; prefer `final` for fields that do not change.
  - Static constants: `public static final` with UPPER_SNAKE_CASE naming.
  - For utility classes, add a private constructor to prevent instantiation.
- Formatting and style:
  - Indentation: 4 spaces.
  - Brace style: opening brace on same line as declaration (K&R).
  - Line length: aim for <= 120 characters.
  - Use single blank line to separate logical blocks inside methods.
  - Use descriptive boolean variable names (e.g., `isEnabled`, `colorfulLogFormatterEnabled`).
- Java version and modern APIs:
  - Target Java 21 compatibility.
  - Prefer modern JDK APIs where appropriate: use `switch` expressions for multi-branch selection, `Objects.requireNonNullElse`, java.time classes, and `HexFormat` for hex conversions.
  - Avoid adding third-party dependencies for simple tasks that can be done with the JDK.
  - Prefer `record` for simple immutable data carriers where it makes sense and public API stability allows it.
- Logging:
  - Follow existing project pattern of `java.util.logging`.
  - For obtaining a logger: `private static final Logger LOGGER = Logger.getLogger(MyClass.class.getName());`
- Tests:
  - Place tests under the appropriate `src/test/java` module and match package naming of the production code.
  - Tests should be self-contained, use expressive variable names, and include descriptive test method names.
- Avoid:
  - Avoid using `var` in public API signatures. Use `var` only in short local contexts where the inferred type is obvious.
  - Avoid wildcard imports.
  - Avoid unnecessary suppression of warnings; if suppression is used, scope it narrowly and justify in a short comment.
- Examples and small templates
  - File header template:
    // SPDX-License-Identifier: Apache-2.0
    package org.hiero.block.example;
    /**
    * Brief single-line class description.
      */
      public final class ExampleClass {
      /** Explanation of constant. */
      public static final String EXAMPLE_CONSTANT = "value";
      /** Constructor prevents instantiation. */
      private ExampleClass() {}
      }
- Spotless and formatting note for Copilot:
  - The repository uses an organization-level Spotless plugin wrapper (`org.hiero.gradle.check.spotless`). Generated files should conform to the above formatting so they pass Spotless checks.
  - If you are adding or editing Gradle files, prefer the repository's existing Gradle plugin usage over adding new formatting plugins unless approved.
- When in doubt:
  - Match nearby files in the same package/module: copy class-level javadoc style, ordering, and formatting from local examples.
