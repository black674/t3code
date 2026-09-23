# OpenCode V2

Install and authenticate OpenCode v2 on the machine running your environment,
then enable **OpenCode V2** in **Settings > Providers**. See
[provider setup](./install.md#providers). T3 Code requires OpenCode v2.0.13 or
newer, including when you connect an existing server. It behaves like
[OpenCode](./providers-opencode.md) in the UI: same approvals, models, skills,
slash commands, rollback, and plan panel.

## Local or external server

Leave **Server URL** empty to let T3 Code use your running v2 service
(discovered automatically) or spawn one when needed. With no password setting,
a spawned server generates its own credentials.

To use an existing OpenCode v2 server, set **Server URL** and its password in
provider settings. If connection or version checks fail, check the URL,
credentials, and OpenCode version, then refresh provider status.

After a lost connection, send another prompt to reconnect to the same session.

## Approvals

OpenCode V2 follows the shared [permission modes](./permission-modes.md).
**Auto** has the same rules as **Supervised** because OpenCode has no AI
approval reviewer. Use **Allow once** for a single request. Denying an action
does not stop the whole turn.

## Refresh models, commands, and skills

After changing an OpenCode login or configuration, use **Refresh provider
status** in **Settings > Providers** for that environment. On mobile, use
**Refresh models** in the thread settings.

Existing threads keep their selected model and options even when it disappears
from the catalog. If OpenCode rejects that model, select an available one and
retry.
