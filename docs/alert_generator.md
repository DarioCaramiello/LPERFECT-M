# `utils/generate_alert.py` — Alert generator for LPERFECT GeoJSON outputs

This utility reads the **GeoJSON enriched by** `utils/output_to_geo.py` and generates **alerts** for features that exceed configurable thresholds.

It can notify:
- **People** via: **Email**, **SMS**, **WhatsApp**, **Telegram**
- **Systems** via: **REST Webhook/API**, **WebSocket**, **MQTT**

Configuration is supported via:
1) a **JSON configuration file** (`--config config.json`), and  
2) **command-line overrides** (CLI options take precedence).

By default it runs in **dry-run** mode (no messages sent). Use `--send` to actually deliver notifications.

---

## 1. What it reads

### Input file
- A GeoJSON `FeatureCollection` produced by `utils/output_to_geo.py`.
- Each feature must have a `properties` object (if not, it will be treated as empty).

### What properties are evaluated
Rules typically reference properties created by `output_to_geo.py`, e.g.:
- `flood_depth_mean`, `flood_depth_max`
- `risk_index_mean`, `risk_index_max`
- `flood_depth_pct_gt_thr`
- `risk_index_class` (if you added it in your pipeline)

You can also write rules on any **custom property** you include in the GeoJSON.

---

## 2. What it produces

- Sends notifications (or logs them in dry-run).
- Optionally writes a deduplication state file (default: `.alerts_sent.json`) so the same alert is not sent repeatedly on multiple runs.

No GeoJSON output is produced; it is a **notifier**.

---

## 3. Installation

Core features use only the standard library + SMTP (stdlib).  
Optional channels require extra packages:

```bash
# Always needed for GeoJSON parsing (stdlib json) -> nothing special

# Optional for WebSocket
pip install websocket-client

# Optional for MQTT
pip install paho-mqtt
```

For SMS/WhatsApp, you need a **Twilio account** (no extra Python package needed; this script calls Twilio REST).

---

## 4. Quick start

### 4.1 Create a config file `config.json`

Start from this minimal example (enable only the channels you plan to use):

```json
{
  "dry_run": true,
  "dedupe": { "enabled": true, "path": ".alerts_sent.json" },

  "contacts": {
    "email_keys": ["email", "contact_email"],
    "phone_keys": ["phone", "telephone", "cell", "mobile", "contact_phone"],
    "telegram_chat_id_keys": ["telegram_chat_id", "tg_chat_id"],

    "rest_url_keys": ["webhook_url", "rest_url", "callback_url"],
    "ws_url_keys": ["ws_url", "websocket_url"],
    "mqtt_url_keys": ["mqtt_url", "mqtt_broker_url"],
    "mqtt_topic_keys": ["mqtt_topic", "topic"],

    "rest_headers_keys": ["rest_headers"],
    "rest_bearer_keys": ["rest_auth_bearer", "bearer_token"],
    "rest_basic_user_keys": ["rest_auth_basic_user", "basic_user"],
    "rest_basic_pass_keys": ["rest_auth_basic_pass", "basic_pass"],

    "mqtt_client_id_keys": ["mqtt_client_id"],
    "mqtt_username_keys": ["mqtt_username"],
    "mqtt_password_keys": ["mqtt_password"],
    "mqtt_tls_ca_keys": ["mqtt_tls_ca"],
    "mqtt_tls_cert_keys": ["mqtt_tls_cert"],
    "mqtt_tls_key_keys": ["mqtt_tls_key"],
    "mqtt_tls_insecure_keys": ["mqtt_tls_insecure"]
  },

  "rules": [
    {
      "name": "High flood depth",
      "property": "flood_depth_max",
      "op": ">=",
      "value": 0.30,
      "severity": "HIGH",
      "channels": ["email", "rest", "mqtt"],
      "message_template": "[{severity}] Flood alert for {feature_name}: flood_depth_max={flood_depth_max} m (thr={rule_value})."
    }
  ],

  "email": {
    "enabled": false,
    "smtp_host": "smtp.example.org",
    "smtp_port": 587,
    "use_tls": true,
    "username": "user@example.org",
    "password": "APP_PASSWORD",
    "from_addr": "LPERFECT Alerts <alerts@example.org>",
    "subject": "LPERFECT Alert"
  },

  "twilio": {
    "enabled": false,
    "account_sid": "ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "auth_token": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    "from_sms": "+1234567890",
    "from_whatsapp": "whatsapp:+1234567890"
  },

  "telegram": {
    "enabled": false,
    "bot_token": "123456:ABC-DEF..."
  },

  "rest": {
    "enabled": true,
    "method": "POST",
    "headers": { "X-Source": "LPERFECT" }
  },

  "websocket": {
    "enabled": true,
    "timeout": 10
  },

  "mqtt": {
    "enabled": true,
    "qos": 0,
    "retain": false,
    "timeout": 10,

    "client_id": null,
    "username": null,
    "password": null,
    "tls_ca": null,
    "tls_cert": null,
    "tls_key": null,
    "tls_insecure": false
  }
}
```

### 4.2 Run a dry-run (recommended first)

```bash
python utils/generate_alert.py \
  --geojson assets_with_risk.geojson \
  --config config.json
```

### 4.3 Actually send notifications

```bash
python utils/generate_alert.py \
  --geojson assets_with_risk.geojson \
  --config config.json \
  --send
```

---

## 5. Rules: how alerts are triggered

Each rule checks **one property** from `feature.properties` using an operator.

### Rule fields
- `name` *(string)*: label used in logs/dedupe
- `property` *(string)*: the property key to inspect (e.g., `flood_depth_max`)
- `op` *(string)*: one of:
  - Numeric: `>`, `>=`, `<`, `<=`, `==`, `!=`
  - Membership: `in` (value must be a list)
  - Text: `contains`
- `value` *(number|string|list)*: comparison value
- `severity` *(string)*: free label (e.g., `LOW`, `MEDIUM`, `HIGH`)
- `channels` *(list of strings)*: which notification channels to attempt
- `message_template` *(string, optional)*: python `.format()` template; can reference:
  - `{feature_name}`
  - `{severity}`, `{rule_value}`, `{actual}`, `{prop}`, `{op}`
  - any key present in feature `properties`, e.g. `{flood_depth_max}`

### Example rules
Trigger on flood depth max:

```json
{
  "name": "FloodDepth>=0.30",
  "property": "flood_depth_max",
  "op": ">=",
  "value": 0.30,
  "severity": "HIGH",
  "channels": ["email", "sms", "telegram"]
}
```

Trigger on risk class:

```json
{
  "name": "High risk class",
  "property": "risk_index_class",
  "op": "in",
  "value": ["R3", "R4"],
  "severity": "HIGH",
  "channels": ["rest", "mqtt", "websocket"]
}
```

Trigger on percent area flooded:

```json
{
  "name": "AreaFlooded>20%",
  "property": "flood_depth_pct_gt_thr",
  "op": ">=",
  "value": 20.0,
  "severity": "MEDIUM",
  "channels": ["email", "telegram"]
}
```

---

## 6. Contacts and endpoint properties (REQUIRED to fire notifications)

A rule may match, but a notification is only delivered if the feature has the required contact/URL
and the channel/provider is enabled in config.

### 6.1 Email channel

**To fire an email notification**, the feature must contain an email property with one of the configured keys (default shown):

- `email` or `contact_email` *(string)*

Example feature properties:

```json
"properties": {
  "name": "Bridge A",
  "email": "ops@mycompany.org"
}
```

Additionally, you must enable email in config:

```json
"email": {
  "enabled": true,
  "smtp_host": "...",
  "smtp_port": 587,
  "use_tls": true,
  "username": "...",
  "password": "...",
  "from_addr": "...",
  "subject": "LPERFECT Alert"
}
```

### 6.2 SMS channel (Twilio)

**To fire an SMS**, the feature must contain a phone number (E.164 is recommended):

- `phone`, `telephone`, `cell`, `mobile`, or `contact_phone` *(string)*

Example:

```json
"properties": { "phone": "+393331234567" }
```

Enable Twilio and provide a sender number:

```json
"twilio": {
  "enabled": true,
  "account_sid": "AC...",
  "auth_token": "...",
  "from_sms": "+1234567890"
}
```

### 6.3 WhatsApp channel (Twilio WhatsApp)

Same phone property requirements as SMS. The script will convert `+39...` to `whatsapp:+39...` automatically.

Enable Twilio WhatsApp sender:

```json
"twilio": {
  "enabled": true,
  "account_sid": "AC...",
  "auth_token": "...",
  "from_whatsapp": "whatsapp:+1234567890"
}
```

### 6.4 Telegram channel

**To fire a Telegram message**, the feature must contain a chat id:

- `telegram_chat_id` or `tg_chat_id` *(string or number)*

Example:

```json
"properties": { "telegram_chat_id": "123456789" }
```

And you must provide a bot token:

```json
"telegram": { "enabled": true, "bot_token": "123456:ABC..." }
```

### 6.5 REST / Webhook channel

**To fire a REST notification**, the feature must contain a URL:

- `webhook_url`, `rest_url`, or `callback_url` *(string URL)*

Example:

```json
"properties": { "webhook_url": "https://example.org/alerts" }
```

Global REST config (method + default headers):

```json
"rest": {
  "enabled": true,
  "method": "POST",
  "headers": { "X-Source": "LPERFECT" }
}
```

#### Per-feature REST headers/auth (optional but supported)

You can attach per-feature auth/headers:

- `rest_headers`: either a **JSON object** or a **JSON string** of an object
- `rest_auth_bearer`: bearer token string
- `rest_auth_basic_user`: basic auth username
- `rest_auth_basic_pass`: basic auth password

Example:

```json
"properties": {
  "webhook_url": "https://api.example.org/alerts",
  "rest_auth_bearer": "eyJhbGciOi...",
  "rest_headers": { "X-Asset": "Bridge-A" }
}
```

If both bearer and basic are set, basic will overwrite the `Authorization` header (last-wins).

#### REST payload format

The script POSTs a JSON document with keys:

- `timestamp` (epoch seconds)
- `feature_id`, `feature_name`
- `rule` object (`name`, `property`, `op`, `value`, `severity`)
- `actual` (the feature property value that triggered)
- `message` (rendered message)
- `properties` (entire feature properties object)
- `geometry` (the feature geometry)

This is designed for easy ingestion into monitoring/incident systems.

### 6.6 WebSocket channel

**To fire a WebSocket notification**, the feature must contain a ws URL:

- `ws_url` or `websocket_url` *(string URL, typically `ws://` or `wss://`)*

Example:

```json
"properties": { "ws_url": "wss://example.org/ws/alerts" }
```

Enable websocket in config:

```json
"websocket": { "enabled": true, "timeout": 10 }
```

Message sent:
- A JSON string containing the **same payload** as REST (`build_payload(...)`).

> Requires: `pip install websocket-client`

### 6.7 MQTT channel

**To fire an MQTT notification**, the feature must contain:
- Broker URL: `mqtt_url` or `mqtt_broker_url`
- Topic: `mqtt_topic` or `topic`

Example:

```json
"properties": {
  "mqtt_url": "mqtt://broker.example.org:1883",
  "mqtt_topic": "lperfect/alerts/bridge_a"
}
```

Global MQTT config:

```json
"mqtt": { "enabled": true, "qos": 0, "retain": false, "timeout": 10 }
```

#### Per-feature MQTT overrides (optional)

You can override MQTT details on a per-feature basis:

- `mqtt_client_id` *(string)*
- `mqtt_username`, `mqtt_password` *(string)*
- `mqtt_tls_ca` *(path)*
- `mqtt_tls_cert` *(path)*
- `mqtt_tls_key` *(path)*
- `mqtt_tls_insecure` *(bool)*

Example (MQTTS + client certs):

```json
"properties": {
  "mqtt_url": "mqtts://broker.example.org:8883",
  "mqtt_topic": "lperfect/alerts/site42",
  "mqtt_client_id": "site42-alerts",
  "mqtt_username": "u",
  "mqtt_password": "p",
  "mqtt_tls_ca": "/etc/ssl/certs/ca.pem",
  "mqtt_tls_cert": "/etc/ssl/certs/client.crt",
  "mqtt_tls_key": "/etc/ssl/private/client.key",
  "mqtt_tls_insecure": false
}
```

Message published:
- A JSON string containing the same payload as REST.

> Requires: `pip install paho-mqtt`

---

## 7. Dedupe: preventing repeated alerts

If enabled, the script stores a record for each sent alert in a local JSON file (default `.alerts_sent.json`).

An alert is identified by a stable hash of:
- feature identifier (`feature.id` or `properties.id`/`properties.name`)
- rule name + expression
- actual value

### Resetting dedupe
Delete the file:

```bash
rm -f .alerts_sent.json
```

Or disable dedupe in config:

```json
"dedupe": { "enabled": false }
```

Or via CLI:

```bash
python utils/generate_alert.py --geojson out.geojson --config config.json --send --no-dedupe
```

---

## 8. Running without a config file (CLI-only)

You can run a quick one-off rule directly from CLI:

```bash
python utils/generate_alert.py \
  --geojson assets_with_risk.geojson \
  --rule "flood_depth_max>=0.25" \
  --channels rest,mqtt \
  --severity HIGH \
  --send
```

Notes:
- CLI rules default to channel `email` unless you set `--channels`.
- Without `--config`, all providers are disabled by default, so **system channels** (rest/websocket/mqtt) work only if enabled by defaults in your local script. In practice, use a config file.

---

## 9. Logging / troubleshooting

Use log levels:
- `INFO`: normal run info
- `WARNING`: alert matches + missing routes
- `DEBUG`: useful when diagnosing configuration

Example:

```bash
python utils/generate_alert.py --geojson out.geojson --config config.json --log-level DEBUG
```

Common issues:
- **Rule matches but no notification is sent**:
  - Missing required contact/URL property for that channel, or provider not enabled.
- **WebSocket/MQTT errors**:
  - You may be missing optional dependencies (`websocket-client`, `paho-mqtt`).
- **SMTP auth fails**:
  - Use an app password for providers like Gmail/Office365; ensure TLS/port correct.
- **Twilio errors**:
  - Verify sender numbers and WhatsApp enablement in Twilio console.

---

## 10. Feature property checklist (copy/paste)

### People channels
- Email: one of `email`, `contact_email`
- Phone (SMS/WhatsApp): one of `phone`, `telephone`, `cell`, `mobile`, `contact_phone`
- Telegram: one of `telegram_chat_id`, `tg_chat_id`

### System channels
- REST: one of `webhook_url`, `rest_url`, `callback_url`
  - Optional: `rest_headers`, `rest_auth_bearer`, `rest_auth_basic_user`, `rest_auth_basic_pass`
- WebSocket: `ws_url` or `websocket_url`
- MQTT: `mqtt_url`/`mqtt_broker_url` **and** `mqtt_topic`/`topic`
  - Optional: `mqtt_client_id`, `mqtt_username`, `mqtt_password`, `mqtt_tls_ca`, `mqtt_tls_cert`, `mqtt_tls_key`, `mqtt_tls_insecure`

---

## License / attribution
Use according to your project license. This README documents the configuration and operation of `utils/generate_alert.py`.
