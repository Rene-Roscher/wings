[![Logo Image](https://cdn.pterodactyl.io/logos/new/pterodactyl_logo.png)](https://pterodactyl.io)

![Discord](https://img.shields.io/discord/122900397965705216?label=Discord&logo=Discord&logoColor=white)
![GitHub Releases](https://img.shields.io/github/downloads/pterodactyl/wings/latest/total)
[![Go Report Card](https://goreportcard.com/badge/github.com/pterodactyl/wings)](https://goreportcard.com/report/github.com/pterodactyl/wings)

# Pterodactyl Wings

Wings is Pterodactyl's server control plane, built for the rapidly changing gaming industry and designed to be
highly performant and secure. Wings provides an HTTP API allowing you to interface directly with running server
instances, fetch server logs, generate backups, and control all aspects of the server lifecycle.

In addition, Wings ships with a built-in SFTP server allowing your system to remain free of Pterodactyl specific
dependencies, and allowing users to authenticate with the same credentials they would normally use to access the Panel.

## Enhanced Features

### Real-time Backup & Restore Progress Tracking

Wings now provides ultra-responsive real-time progress tracking for backup creation and restoration operations via WebSocket events:

#### Backup Progress Events
- **Live percentage tracking** with intelligent size estimation
- **Real-time byte counters** showing data processed  
- **200ms update intervals** for maximum responsiveness without spam
- **Smart throttling** prevents WebSocket overload while maintaining live feel
- **Fallback to bytes-only mode** when size estimation unavailable

#### Restore Progress Events  
- **File-by-file progress tracking** with 100ms update intervals
- **Real-time restoration status** showing files being processed
- **Intelligent progress calculation** based on backup file size estimation
- **Ultra-live updates** for immediate user feedback

#### Server State Management
- **New server states**: `backup` and `restore` for clear operation visibility
- **Smart state restoration** automatically detects actual container state after operations
- **WebSocket state events** keep frontend synchronized with server status
- **Robust state handling** prevents race conditions during concurrent operations

#### Event Payloads
All progress events include comprehensive information:
```json
{
  "backup_id": "47363ce7-d70a-430e-8e75-6dc87c8d016d",
  "type": "create|restore", 
  "percentage": 45,
  "bytes_written": 1048576,
  "bytes_total": 2097152
}
```

#### Performance Characteristics
- **Zero performance impact** on backup/restore operations
- **Ultra-lightweight tracking** with atomic operations only
- **Async event publishing** never blocks file operations  
- **Smart size estimation** uses cached disk usage when available
- **Graceful degradation** maintains functionality even with estimation failures

### Enhanced Activity Logging

- **Complete file operation tracking** for SFTP, HTTP API, and console commands
- **Real-time WebSocket events** for all file system changes
- **Comprehensive activity metadata** including file paths, users, and operation types
- **Automatic event publishing** with panic recovery for maximum reliability

## Sponsors

I would like to extend my sincere thanks to the following sponsors for helping fund Pterodactyl's development.
[Interested in becoming a sponsor?](https://github.com/sponsors/matthewpi)

| Company                                                                           | About                                                                                                                                                                                                                                           |
|-----------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [**Aussie Server Hosts**](https://aussieserverhosts.com/)                         | No frills Australian Owned and operated High Performance Server hosting for some of the most demanding games serving Australia and New Zealand.                                                                                                 |
| [**BisectHosting**](https://www.bisecthosting.com/)                               | BisectHosting provides Minecraft, Valheim and other server hosting services with the highest reliability and lightning fast support since 2012.                                                                                                 |
| [**MineStrator**](https://minestrator.com/)                                       | Looking for the most highend French hosting company for your minecraft server? More than 24,000 members on our discord trust us. Give us a try!                                                                                                 |
| [**HostEZ**](https://hostez.io)                                                   | US & EU Rust & Minecraft Hosting. DDoS Protected bare metal, VPS and colocation with low latency, high uptime and maximum availability. EZ!                                                                                                     |
| [**Blueprint**](https://blueprint.zip/?utm_source=pterodactyl&utm_medium=sponsor) | Create and install Pterodactyl addons and themes with the growing Blueprint framework - the package-manager for Pterodactyl. Use multiple modifications at once without worrying about conflicts and make use of the large extension ecosystem. |
| [**indifferent broccoli**](https://indifferentbroccoli.com/)                      | indifferent broccoli is a game server hosting and rental company. With us, you get top-notch computer power for your gaming sessions. We destroy lag, latency, and complexity--letting you focus on the fun stuff.                              |

## Documentation

* [Panel Documentation](https://pterodactyl.io/panel/1.0/getting_started.html)
* [Wings Documentation](https://pterodactyl.io/wings/1.0/installing.html)
* [Community Guides](https://pterodactyl.io/community/about.html)
* Or, get additional help [via Discord](https://discord.gg/pterodactyl)

## Reporting Issues

Please use the [pterodactyl/panel](https://github.com/pterodactyl/panel) repository to report any issues or make
feature requests for Wings. In addition, the [security policy](https://github.com/pterodactyl/panel/security/policy) listed
within that repository also applies to Wings.
