# Block Persistence

## Overview

Block persistence is a core feature of the Block Node application. It ensures that blocks are verified and stored efficiently.

## Key Classes

- `BlocksFilesRecentPlugin`: Handles block persistence and deletion logic.
- `BlockFile`: Provides utilities for managing block file paths.

## Logic Flow

1. **Verification**: Blocks are verified before being persisted.
2. **Storage**: Verified blocks are written to disk using `writeBlockToLivePath`.
3. **Notification**: A `PersistedNotification` is sent after a block is stored.
4. **Cleanup**: Blocks are deleted if another plugin with a lower priority stores them.

## Patterns Used

- **Observer Pattern**: Notifications are sent to other components when blocks are persisted.
- **File Management**: Uses Java NIO for file operations.
