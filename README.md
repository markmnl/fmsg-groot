# fmsg-groot

I am Groot.

A small [fmsg](https://github.com/markmnl/fmsg) bot that listens on an
[fmsg-webapi](https://github.com/markmnl/fmsg-webapi) inbox and replies to every
message with a prepared **“I am Groot”** line — punctuation, CAPS, and spacing
vary for “emotion” (Guardians of the Galaxy style).

Replies go to **all participants** on the parent message (minus itself). If the
inbound body already contains `groot` (case-insensitive), it answers once with
`I am Groot too!`. If the body already contains `groot too!`, it stays quiet so
two Groots do not chat forever.

## Requirements

- Go 1.24 or newer (to build)
- An fmsg Web API base URL and an API key (`fmsgk_…`) for the address that
  should speak as Groot

## Build

```sh
go build -o fmsg-groot
```

## Run

```sh
export FMSG_API_URL=https://api.example.com
export FMSG_API_KEY=fmsgk_your_key_here
# optional: FMSG_GROOT_STATE=./fmsg-groot.state
./fmsg-groot
```

Flags `-api-url`, `-api-key`, and `-state` override the env vars.

The process authenticates via `POST /fmsg/token`, watches `GET /fmsg/ws` (with
HTTP inbox catch-up), and sends replies as drafts through the Web API. See
[FMSG-003](https://github.com/markmnl/fmsg/blob/main/standards/fmsg-003-webapi.md).

## systemd

Concise install as a service (paths match other fmsg host services):

1. Build and install the binary:

   ```sh
   go build -o fmsg-groot
   install -d /opt/fmsg-groot /var/lib/fmsg-groot
   install -m 755 fmsg-groot /opt/fmsg-groot/fmsg-groot
   chown -R fmsg:fmsg /opt/fmsg-groot /var/lib/fmsg-groot
   ```

2. Env file `/opt/fmsg-groot/env` (mode `0600`):

   ```
   FMSG_API_URL=https://api.example.com
   FMSG_API_KEY=fmsgk_your_key_here
   FMSG_GROOT_STATE=/var/lib/fmsg-groot/state
   ```

3. Unit file — copy [`contrib/fmsg-groot.service`](contrib/fmsg-groot.service)
   to `/etc/systemd/system/fmsg-groot.service` (or adapt User/paths).

4. Enable and start:

   ```sh
   systemctl daemon-reload
   systemctl enable --now fmsg-groot
   systemctl status fmsg-groot
   ```

## License

MIT
