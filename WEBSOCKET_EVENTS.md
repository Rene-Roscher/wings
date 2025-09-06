# Wings WebSocket Events Reference

This document provides a complete reference for all WebSocket events emitted by Pterodactyl Wings.

## Event Overview

All events are sent via WebSocket in the following format:
```json
{
  "event": "event_name", 
  "args": [payload]
}
```

## Server Events

### 🏃 **Runtime & Process Events**

#### `status`
Server status changes (starting, running, stopping, offline).
```json
{
  "event": "status",
  "args": ["running"]
}
```
**Values**: `offline`, `starting`, `running`, `stopping`

#### `stats` 
Server resource usage statistics.
```json
{
  "event": "stats",
  "args": [{
    "memory_bytes": 1073741824,
    "memory_limit_bytes": 2147483648,
    "cpu_absolute": 45.5,
    "network": {
      "rx_bytes": 1024,
      "tx_bytes": 2048
    },
    "uptime": 3600000,
    "state": "running"
  }]
}
```

#### `console output`
Real-time console output from the server process.
```json
{
  "event": "console output",
  "args": ["[10:30:15] [Server thread/INFO]: Player joined the game"]
}
```

#### `daemon message`
System messages from Wings daemon.
```json
{
  "event": "daemon message", 
  "args": ["Server marked as starting..."]
}
```

### 📦 **Installation Events**

#### `install started`
Server installation process has begun.
```json
{
  "event": "install started",
  "args": [true]
}
```

#### `install output`
Real-time output from installation process.
```json
{
  "event": "install output",
  "args": ["Downloading server files..."]
}
```

#### `install completed`
Installation process finished.
```json
{
  "event": "install completed",
  "args": [true]
}
```

### 💾 **Backup Events**

#### `backup progress` ⭐ *New*
Real-time backup creation/restoration progress.
```json
{
  "event": "backup progress",
  "args": [{
    "type": "create",
    "percentage": 45,
    "bytes_written": 471859200,
    "bytes_total": 1048576000
  }]
}
```

**Fields:**
- `type`: `"create"` or `"restore"`
- `percentage`: 0-100, or -1 for error
- `bytes_written`: Bytes processed
- `bytes_total`: Total bytes (create only)

#### `backup completed`
Backup creation finished.
```json
{
  "event": "backup completed",
  "args": [{
    "uuid": "backup-uuid",
    "is_successful": true,
    "checksum": "sha1-hash",
    "checksum_type": "sha1", 
    "file_size": 1048576000
  }]
}
```

#### `backup restore completed`
Backup restoration finished.
```json
{
  "event": "backup restore completed",
  "args": [true]
}
```

### 📁 **Activity Events** ⭐ *Enhanced*

#### `activity`
File operations and server activities.
```json
{
  "event": "activity",
  "args": [{
    "event": "server:file.write",
    "user": "user-uuid",
    "metadata": {
      "file": "config/server.properties"
    }
  }]
}
```

**Common Activity Types:**
- `server:file.write` - File created/modified
- `server:file.delete` - File deleted  
- `server:file.rename` - File renamed
- `server:file.create-directory` - Directory created
- `server:file.compress` - Files compressed
- `server:file.decompress` - Archive extracted
- `server:console.command` - Console command executed
- `server:power.start` - Server started
- `server:power.stop` - Server stopped

### 🔄 **Transfer Events**

#### `transfer logs`
Server transfer process logs.
```json
{
  "event": "transfer logs",
  "args": ["Transferring server files..."]
}
```

#### `transfer status`
Transfer status updates.
```json
{
  "event": "transfer status", 
  "args": ["processing"]
}
```

### ❌ **Deletion Events**

#### `deleted`
Server has been deleted from the system.
```json
{
  "event": "deleted",
  "args": [null]
}
```

## Event Frequency & Performance

| Event Type | Frequency | Throttling |
|------------|-----------|------------|
| `status` | On state change | None |
| `stats` | Every 1-2 seconds | Built-in |
| `console output` | Real-time | None |
| `backup progress` | Max 2/second | 500ms throttling |
| `activity` | On action | None |
| `install output` | Real-time | None |
| `daemon message` | As needed | None |

## Authentication

WebSocket connections require JWT authentication:
```
wss://wings-host/api/servers/{server-id}/ws?token=jwt-token
```

## Error Handling

All events are sent asynchronously. If event publishing fails, server operations continue normally. Events are not queued or retried.

## Backwards Compatibility  

All events maintain backwards compatibility. New fields may be added but existing fields will not be removed or changed in structure.