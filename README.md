# fmsg-groot

I am Groot.

A small [fmsg](https://github.com/markmnl/fmsg) bot and A2A agent that listens
on an [fmsg-webapi](https://github.com/markmnl/fmsg-webapi) inbox and replies
with a prepared **“I am Groot”** line — punctuation, CAPS, and spacing vary for
“emotion” (Guardians of the Galaxy style).

Replies go to **all participants** on the parent message (minus itself). If the
inbound body already contains `groot` (case-insensitive), it answers once with
`I am Groot too!`. If the body already contains `groot too!`, it stays quiet so
two Groots do not chat forever.

Groot also implements the server side of
[FMSG-004 v0.2.0](https://github.com/markmnl/fmsg/blob/main/standards/fmsg-004-a2a-binding.md)
for A2A 1.0. Send an A2A `SendMessage` request to Groot's fmsg address with the
media type `application/vnd.fmsg.a2a+json`; it returns a direct A2A `Message`.
The implementation is deliberately stateless at the A2A layer: `ListTasks`
returns an empty page, task lookups return `A2A_TASK_NOT_FOUND`, and streaming,
push notifications, and extended Agent Cards return their prescribed
unsupported errors. Those capabilities must be advertised as `false` in the
Agent Card.

The binding validates the fmsg message profile, envelopes, payloads, thread
correlation, and native `Part.raw` attachment mapping. The verified fmsg sender
address is the transport principal; Groot advertises no additional A2A
security requirement and does not accept an envelope `credentials` object.

## Requirements

- Go 1.26.4 or newer (to build)
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
# optional: FMSG_GROOT_A2A_STATE=./fmsg-groot.state.a2a
./fmsg-groot
```

Flags `-api-url`, `-api-key`, `-state`, and `-a2a-state` override the env vars.
The A2A state file retains request replay and `messageId` idempotency records
for seven days (up to 10,000 of each), plus replies awaiting delivery
confirmation. Keep it on persistent storage and private; it can contain A2A
message content.

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
   FMSG_GROOT_A2A_STATE=/var/lib/fmsg-groot/a2a-state.json
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
