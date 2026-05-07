# redis-cli Compatibility Notes

This document captures the baseline redis-cli startup/introspection command flow we support so interactive troubleshooting remains usable.

## Typical Command Flow

Observed/expected redis-cli sessions may send one or more of these before data commands:

1. `HELLO 3`
2. `CLIENT SETINFO LIB-NAME redis-cli`
3. `CLIENT SETINFO LIB-VER <version>`
4. `COMMAND` / `COMMAND DOCS` / `COMMAND INFO`
5. `SELECT <db>`

After preflight, users commonly execute:

- `PING`
- `SET <key> <value>`
- `GET <key>`
- `KEYS *`

## Supported Compatibility Commands

- `PING [message]`
- `ECHO <message>`
- `COMMAND`, `COMMAND COUNT`, `COMMAND INFO`, `COMMAND DOCS`, `COMMAND GETKEYS`
- `HELLO [2|3]`
- `CLIENT SETINFO <field> <value>`
- `CLIENT SETNAME <name>`
- `CLIENT GETNAME`
- `CLIENT ID`
- `SELECT <db>`
- `QUIT`

Unsupported commands or subcommands return Redis-style error responses.
