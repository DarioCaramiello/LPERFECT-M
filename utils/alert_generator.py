#!/usr/bin/env python3
"""
utils/generate_alert.py

Reads a GeoJSON produced by utils/output_to_geo.py, evaluates threshold rules,
and sends alerts to contacts or endpoints found in feature.properties.

Channels:
- email (SMTP)
- sms (Twilio)
- whatsapp (Twilio WhatsApp)
- telegram (Telegram Bot API)
- rest (invoke REST endpoint from feature properties)
- websocket (send JSON payload to WebSocket URL from feature properties)
- mqtt (publish JSON payload to MQTT broker/topic from feature properties)

Config:
- JSON file via --config
- CLI overrides JSON

New in this version:
- REST per-feature headers/auth:
    rest_headers (dict or JSON string)
    rest_auth_bearer
    rest_auth_basic_user + rest_auth_basic_pass
- MQTT per-feature TLS/auth/client settings:
    mqtt_client_id
    mqtt_username / mqtt_password
    mqtt_tls_ca / mqtt_tls_cert / mqtt_tls_key
    mqtt_tls_insecure (true/false)

Dependencies:
- Standard library: works for email/rest (urllib)
- Optional for websocket/mqtt:
    pip install websocket-client
    pip install paho-mqtt
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import os
import smtplib
import ssl
import time
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Any, Dict, List, Optional, Tuple

import urllib.request
import urllib.parse

LOG = logging.getLogger("generate_alert")


# -----------------------------
# Rules
# -----------------------------
@dataclass(frozen=True)
class Rule:
    name: str
    prop: str
    op: str
    value: Any
    severity: str = "MEDIUM"
    channels: List[str] = None
    message_template: str = "[{severity}] Alert for {feature_name}: {prop} {op} {rule_value} (actual={actual})."


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _compare(actual: Any, op: str, rule_value: Any) -> bool:
    op = op.strip()

    if op in (">", ">=", "<", "<=", "==", "!="):
        a = _to_float(actual)
        b = _to_float(rule_value)
        if a is None or b is None:
            return False
        if op == ">":
            return a > b
        if op == ">=":
            return a >= b
        if op == "<":
            return a < b
        if op == "<=":
            return a <= b
        if op == "==":
            return a == b
        if op == "!=":
            return a != b

    if op == "in":
        return isinstance(rule_value, list) and actual in rule_value

    if op == "contains":
        return actual is not None and str(rule_value) in str(actual)

    return False


def parse_rule_expr(expr: str) -> Rule:
    expr = expr.strip()
    if " in " in expr:
        left, right = expr.split(" in ", 1)
        prop = left.strip()
        vals = [v.strip() for v in right.split(",") if v.strip()]
        return Rule(name=f"{prop} in {vals}", prop=prop, op="in", value=vals, channels=["email"])

    for op in [">=", "<=", "!=", "==", ">", "<"]:
        if op in expr:
            left, right = expr.split(op, 1)
            prop = left.strip()
            raw = right.strip()
            val: Any = _to_float(raw) if _to_float(raw) is not None else raw
            return Rule(name=f"{prop}{op}{raw}", prop=prop, op=op, value=val, channels=["email"])

    raise ValueError(f"Cannot parse rule expression: {expr}")


# -----------------------------
# Keys in feature properties
# -----------------------------
@dataclass(frozen=True)
class Keys:
    email_keys: List[str]
    phone_keys: List[str]
    telegram_chat_id_keys: List[str]
    rest_url_keys: List[str]
    ws_url_keys: List[str]
    mqtt_url_keys: List[str]
    mqtt_topic_keys: List[str]

    # Per-feature REST auth/headers keys
    rest_headers_keys: List[str]
    rest_bearer_keys: List[str]
    rest_basic_user_keys: List[str]
    rest_basic_pass_keys: List[str]

    # Per-feature MQTT options keys
    mqtt_client_id_keys: List[str]
    mqtt_username_keys: List[str]
    mqtt_password_keys: List[str]
    mqtt_tls_ca_keys: List[str]
    mqtt_tls_cert_keys: List[str]
    mqtt_tls_key_keys: List[str]
    mqtt_tls_insecure_keys: List[str]


def get_first_prop(props: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for k in keys:
        v = props.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


def get_any_prop(props: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in props:
            return props.get(k)
    return None


# -----------------------------
# Email (SMTP)
# -----------------------------
def send_email_smtp(
    smtp_host: str,
    smtp_port: int,
    use_tls: bool,
    username: Optional[str],
    password: Optional[str],
    from_addr: str,
    to_addr: str,
    subject: str,
    body: str,
    timeout: int = 20,
) -> None:
    msg = EmailMessage()
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.set_content(body)

    if use_tls:
        context = ssl.create_default_context()
        with smtplib.SMTP(smtp_host, smtp_port, timeout=timeout) as s:
            s.starttls(context=context)
            if username and password:
                s.login(username, password)
            s.send_message(msg)
    else:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=timeout) as s:
            if username and password:
                s.login(username, password)
            s.send_message(msg)


# -----------------------------
# Twilio (SMS / WhatsApp)
# -----------------------------
def twilio_send_message(
    account_sid: str,
    auth_token: str,
    from_number: str,
    to_number: str,
    body: str,
    timeout: int = 20,
) -> None:
    url = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json"
    data = urllib.parse.urlencode({"From": from_number, "To": to_number, "Body": body}).encode("utf-8")

    req = urllib.request.Request(url, data=data, method="POST")
    auth = (f"{account_sid}:{auth_token}").encode("utf-8")
    req.add_header("Authorization", "Basic " + base64.b64encode(auth).decode("ascii"))
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        _ = resp.read()


# -----------------------------
# Telegram Bot API
# -----------------------------
def telegram_send_message(bot_token: str, chat_id: str, text: str, timeout: int = 20) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        _ = resp.read()


# -----------------------------
# REST Webhook / API call
# -----------------------------
def _parse_headers_maybe(v: Any) -> Dict[str, str]:
    """
    Accept dict or JSON string representing dict.
    """
    if v is None:
        return {}
    if isinstance(v, dict):
        return {str(k): str(val) for k, val in v.items()}
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return {}
        try:
            obj = json.loads(s)
            if isinstance(obj, dict):
                return {str(k): str(val) for k, val in obj.items()}
        except Exception:
            return {}
    return {}


def build_rest_headers(
    global_headers: Dict[str, str],
    feature_headers: Dict[str, str],
    bearer: Optional[str],
    basic_user: Optional[str],
    basic_pass: Optional[str],
) -> Dict[str, str]:
    """
    Merge headers (feature overrides global), then apply auth if provided.
    """
    headers = dict(global_headers or {})
    headers.update(feature_headers or {})

    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"

    if basic_user and basic_pass:
        token = base64.b64encode(f"{basic_user}:{basic_pass}".encode("utf-8")).decode("ascii")
        headers["Authorization"] = f"Basic {token}"

    return headers


def rest_notify(
    url: str,
    payload: Dict[str, Any],
    method: str = "POST",
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 20,
) -> None:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method.upper())
    req.add_header("Content-Type", "application/json")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        _ = resp.read()


# -----------------------------
# WebSocket notify (optional websocket-client)
# -----------------------------
def websocket_notify(ws_url: str, message: str, timeout: int = 10) -> None:
    try:
        import websocket  # type: ignore
    except ImportError as e:
        raise RuntimeError("websocket-client not installed. Install: pip install websocket-client") from e

    ws = websocket.create_connection(ws_url, timeout=timeout)
    try:
        ws.send(message)
    finally:
        ws.close()


# -----------------------------
# MQTT publish (optional paho-mqtt)
# -----------------------------
def mqtt_publish(
    mqtt_url: str,
    topic: str,
    payload: str,
    qos: int = 0,
    retain: bool = False,
    client_id: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    tls_ca: Optional[str] = None,
    tls_cert: Optional[str] = None,
    tls_key: Optional[str] = None,
    tls_insecure: bool = False,
    timeout: int = 10,
) -> None:
    try:
        import paho.mqtt.client as mqtt  # type: ignore
    except ImportError as e:
        raise RuntimeError("paho-mqtt not installed. Install: pip install paho-mqtt") from e

    from urllib.parse import urlparse

    u = urlparse(mqtt_url)
    if u.scheme not in ("mqtt", "mqtts"):
        raise ValueError("mqtt_url must start with mqtt:// or mqtts://")

    host = u.hostname or "localhost"
    port = u.port or (8883 if u.scheme == "mqtts" else 1883)

    # Create MQTT client (client_id optional but useful for brokers that want stable IDs)
    client = mqtt.Client(client_id=client_id or "")

    if username:
        client.username_pw_set(username, password=password)

    # TLS configuration:
    # - If mqtts:// OR cert paths provided, enable TLS.
    use_tls = (u.scheme == "mqtts") or any([tls_ca, tls_cert, tls_key])
    if use_tls:
        # If tls_ca is None, paho will try default system CAs if you call tls_set() without args.
        if tls_ca or tls_cert or tls_key:
            client.tls_set(ca_certs=tls_ca, certfile=tls_cert, keyfile=tls_key)
        else:
            client.tls_set()
        client.tls_insecure_set(bool(tls_insecure))

    # One-shot connect/publish/disconnect
    client.connect(host, port, keepalive=30)
    client.loop_start()
    try:
        info = client.publish(topic, payload=payload, qos=qos, retain=retain)
        info.wait_for_publish(timeout=timeout)
    finally:
        client.loop_stop()
        client.disconnect()


# -----------------------------
# Dedupe
# -----------------------------
def load_dedupe(path: str) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {"sent": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"sent": {}}


def save_dedupe(path: str, state: Dict[str, Any]) -> None:
    if not path:
        return
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def make_alert_id(feature: Dict[str, Any], rule: Rule, actual: Any) -> str:
    props = feature.get("properties") or {}
    fid = feature.get("id") or props.get("id") or props.get("name") or props.get("feature_name") or ""
    actual_key = f"{float(actual):.6g}" if isinstance(actual, (int, float)) else str(actual)
    raw = f"{fid}|{rule.name}|{rule.prop}|{rule.op}|{rule.value}|{actual_key}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# -----------------------------
# Formatting / payload
# -----------------------------
def feature_name(feature: Dict[str, Any]) -> str:
    props = feature.get("properties") or {}
    return str(props.get("name") or props.get("feature_name") or props.get("id") or feature.get("id") or "unknown-feature")


def render_message(rule: Rule, feature: Dict[str, Any], actual: Any) -> str:
    props = feature.get("properties") or {}
    ctx = {
        "severity": rule.severity,
        "rule_name": rule.name,
        "prop": rule.prop,
        "op": rule.op,
        "rule_value": rule.value,
        "actual": actual,
        "feature_name": feature_name(feature),
        **props,
    }
    try:
        return rule.message_template.format(**ctx)
    except Exception:
        return f"[{rule.severity}] Alert for {feature_name(feature)}: {rule.prop} {rule.op} {rule.value} (actual={actual})."


def build_payload(rule: Rule, feature: Dict[str, Any], actual: Any, message: str) -> Dict[str, Any]:
    return {
        "timestamp": time.time(),
        "feature_id": feature.get("id"),
        "feature_name": feature_name(feature),
        "rule": {
            "name": rule.name,
            "property": rule.prop,
            "op": rule.op,
            "value": rule.value,
            "severity": rule.severity,
        },
        "actual": actual,
        "message": message,
        "properties": feature.get("properties") or {},
        "geometry": feature.get("geometry"),
    }


# -----------------------------
# Config helpers
# -----------------------------
def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_config(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# -----------------------------
# Main
# -----------------------------
def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generate alerts from output_to_geo.py GeoJSON.")
    p.add_argument("--geojson", required=True)
    p.add_argument("--config", default=None)
    p.add_argument("--send", action="store_true", help="Actually send messages (default dry-run).")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    p.add_argument("--rule", action="append", default=[], help='Rule expression, e.g. "flood_depth_max>=0.25"')
    p.add_argument("--channels", default=None, help="Comma-separated channels for CLI rules")
    p.add_argument("--severity", default=None)
    p.add_argument("--message-template", default=None)

    p.add_argument("--dedupe-file", default=None)
    p.add_argument("--no-dedupe", action="store_true")
    return p


def main() -> int:
    args = build_argparser().parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    defaults: Dict[str, Any] = {
        "dry_run": True,
        "dedupe": {"enabled": True, "path": ".alerts_sent.json"},

        "contacts": {
            "email_keys": ["email", "contact_email"],
            "phone_keys": ["phone", "telephone", "cell", "mobile", "contact_phone"],
            "telegram_chat_id_keys": ["telegram_chat_id", "tg_chat_id"],

            "rest_url_keys": ["webhook_url", "rest_url", "callback_url"],
            "ws_url_keys": ["ws_url", "websocket_url"],
            "mqtt_url_keys": ["mqtt_url", "mqtt_broker_url"],
            "mqtt_topic_keys": ["mqtt_topic", "topic"],

            # REST per-feature auth/headers
            "rest_headers_keys": ["rest_headers"],
            "rest_bearer_keys": ["rest_auth_bearer", "bearer_token"],
            "rest_basic_user_keys": ["rest_auth_basic_user", "basic_user"],
            "rest_basic_pass_keys": ["rest_auth_basic_pass", "basic_pass"],

            # MQTT per-feature options
            "mqtt_client_id_keys": ["mqtt_client_id"],
            "mqtt_username_keys": ["mqtt_username"],
            "mqtt_password_keys": ["mqtt_password"],
            "mqtt_tls_ca_keys": ["mqtt_tls_ca"],
            "mqtt_tls_cert_keys": ["mqtt_tls_cert"],
            "mqtt_tls_key_keys": ["mqtt_tls_key"],
            "mqtt_tls_insecure_keys": ["mqtt_tls_insecure"],
        },

        "rules": [],

        "email": {"enabled": False},
        "twilio": {"enabled": False},
        "telegram": {"enabled": False},

        "rest": {"enabled": True, "method": "POST", "headers": {}},
        "websocket": {"enabled": True, "timeout": 10},

        "mqtt": {
            "enabled": True,
            "qos": 0,
            "retain": False,
            "timeout": 10,
            # global defaults (can be overridden per-feature)
            "client_id": None,
            "username": None,
            "password": None,
            "tls_ca": None,
            "tls_cert": None,
            "tls_key": None,
            "tls_insecure": False,
        },
    }

    cfg = deep_merge(defaults, load_config(args.config))
    cfg["dry_run"] = not args.send

    if args.no_dedupe:
        cfg["dedupe"]["enabled"] = False
    if args.dedupe_file is not None:
        cfg["dedupe"]["path"] = args.dedupe_file

    LOG.info("Reading GeoJSON: %s", args.geojson)
    with open(args.geojson, "r", encoding="utf-8") as f:
        gj = json.load(f)
    if gj.get("type") != "FeatureCollection":
        raise ValueError("Expected a GeoJSON FeatureCollection.")
    features: List[Dict[str, Any]] = gj.get("features") or []
    LOG.info("Features: %d", len(features))

    ckeys = cfg["contacts"]
    keys = Keys(
        email_keys=list(ckeys["email_keys"]),
        phone_keys=list(ckeys["phone_keys"]),
        telegram_chat_id_keys=list(ckeys["telegram_chat_id_keys"]),
        rest_url_keys=list(ckeys["rest_url_keys"]),
        ws_url_keys=list(ckeys["ws_url_keys"]),
        mqtt_url_keys=list(ckeys["mqtt_url_keys"]),
        mqtt_topic_keys=list(ckeys["mqtt_topic_keys"]),
        rest_headers_keys=list(ckeys["rest_headers_keys"]),
        rest_bearer_keys=list(ckeys["rest_bearer_keys"]),
        rest_basic_user_keys=list(ckeys["rest_basic_user_keys"]),
        rest_basic_pass_keys=list(ckeys["rest_basic_pass_keys"]),
        mqtt_client_id_keys=list(ckeys["mqtt_client_id_keys"]),
        mqtt_username_keys=list(ckeys["mqtt_username_keys"]),
        mqtt_password_keys=list(ckeys["mqtt_password_keys"]),
        mqtt_tls_ca_keys=list(ckeys["mqtt_tls_ca_keys"]),
        mqtt_tls_cert_keys=list(ckeys["mqtt_tls_cert_keys"]),
        mqtt_tls_key_keys=list(ckeys["mqtt_tls_key_keys"]),
        mqtt_tls_insecure_keys=list(ckeys["mqtt_tls_insecure_keys"]),
    )

    # Build rules from config + CLI
    rules: List[Rule] = []
    for r in cfg.get("rules", []):
        rules.append(
            Rule(
                name=r.get("name", f"{r.get('property')} {r.get('op')} {r.get('value')}"),
                prop=r["property"],
                op=r["op"],
                value=r["value"],
                severity=r.get("severity", "MEDIUM"),
                channels=r.get("channels", ["email"]),
                message_template=r.get("message_template")
                or "[{severity}] Alert for {feature_name}: {prop} {op} {rule_value} (actual={actual}).",
            )
        )

    if args.rule:
        ch = [c.strip() for c in (args.channels or "email").split(",") if c.strip()]
        for expr in args.rule:
            rr = parse_rule_expr(expr)
            rules.append(
                Rule(
                    name=rr.name,
                    prop=rr.prop,
                    op=rr.op,
                    value=rr.value,
                    severity=args.severity or "MEDIUM",
                    channels=ch,
                    message_template=args.message_template
                    or "[{severity}] Alert for {feature_name}: {prop} {op} {rule_value} (actual={actual}).",
                )
            )

    if not rules:
        LOG.warning("No rules configured. Nothing to do.")
        return 0

    # Dedupe state
    dedupe_enabled = bool(cfg["dedupe"].get("enabled", True))
    dedupe_path = str(cfg["dedupe"].get("path", ".alerts_sent.json"))
    dedupe_state = load_dedupe(dedupe_path) if dedupe_enabled else {"sent": {}}
    sent_map: Dict[str, Any] = dedupe_state.setdefault("sent", {})

    # Provider configs
    email_cfg = cfg.get("email", {})
    twilio_cfg = cfg.get("twilio", {})
    telegram_cfg = cfg.get("telegram", {})
    rest_cfg = cfg.get("rest", {})
    ws_cfg = cfg.get("websocket", {})
    mqtt_cfg = cfg.get("mqtt", {})

    matched = delivered = skipped_dedupe = missing_route = 0

    for feat in features:
        props = feat.get("properties") or {}

        # Contacts
        email = get_first_prop(props, keys.email_keys)
        phone = get_first_prop(props, keys.phone_keys)
        tg_chat = get_first_prop(props, keys.telegram_chat_id_keys)

        # Endpoints/URLs
        rest_url = get_first_prop(props, keys.rest_url_keys)
        ws_url = get_first_prop(props, keys.ws_url_keys)
        mqtt_url = get_first_prop(props, keys.mqtt_url_keys)
        mqtt_topic = get_first_prop(props, keys.mqtt_topic_keys)

        # Per-feature REST auth/headers
        rest_headers_raw = get_any_prop(props, keys.rest_headers_keys)
        feature_rest_headers = _parse_headers_maybe(rest_headers_raw)
        rest_bearer = get_first_prop(props, keys.rest_bearer_keys)
        rest_basic_user = get_first_prop(props, keys.rest_basic_user_keys)
        rest_basic_pass = get_first_prop(props, keys.rest_basic_pass_keys)

        # Per-feature MQTT overrides
        feat_mqtt_client_id = get_first_prop(props, keys.mqtt_client_id_keys)
        feat_mqtt_username = get_first_prop(props, keys.mqtt_username_keys)
        feat_mqtt_password = get_first_prop(props, keys.mqtt_password_keys)
        feat_mqtt_tls_ca = get_first_prop(props, keys.mqtt_tls_ca_keys)
        feat_mqtt_tls_cert = get_first_prop(props, keys.mqtt_tls_cert_keys)
        feat_mqtt_tls_key = get_first_prop(props, keys.mqtt_tls_key_keys)
        feat_mqtt_tls_insecure_raw = get_any_prop(props, keys.mqtt_tls_insecure_keys)

        feat_mqtt_tls_insecure = bool(feat_mqtt_tls_insecure_raw) if feat_mqtt_tls_insecure_raw is not None else None

        for rule in rules:
            actual = props.get(rule.prop)
            if not _compare(actual, rule.op, rule.value):
                continue

            matched += 1
            msg = render_message(rule, feat, actual)
            payload = build_payload(rule, feat, actual, msg)
            alert_id = make_alert_id(feat, rule, actual)

            if dedupe_enabled and alert_id in sent_map:
                skipped_dedupe += 1
                LOG.info("DEDUPE skip: %s | %s", feature_name(feat), rule.name)
                continue

            desired = [c.lower() for c in (rule.channels or [])]
            possible: List[str] = []

            # People channels
            if "email" in desired and email and email_cfg.get("enabled", False):
                possible.append("email")
            if "sms" in desired and phone and twilio_cfg.get("enabled", False):
                possible.append("sms")
            if "whatsapp" in desired and phone and twilio_cfg.get("enabled", False):
                possible.append("whatsapp")
            if "telegram" in desired and tg_chat and telegram_cfg.get("enabled", False):
                possible.append("telegram")

            # System channels
            if "rest" in desired and rest_url and rest_cfg.get("enabled", True):
                possible.append("rest")
            if "websocket" in desired and ws_url and ws_cfg.get("enabled", True):
                possible.append("websocket")
            if "mqtt" in desired and mqtt_url and mqtt_topic and mqtt_cfg.get("enabled", True):
                possible.append("mqtt")

            if not possible:
                missing_route += 1
                LOG.warning(
                    "No deliverable route for %s (rule=%s). Have email=%s phone=%s tg=%s rest=%s ws=%s mqtt=%s topic=%s",
                    feature_name(feat), rule.name, bool(email), bool(phone), bool(tg_chat),
                    bool(rest_url), bool(ws_url), bool(mqtt_url), bool(mqtt_topic)
                )
                continue

            LOG.warning("ALERT match: %s | rule=%s | channels=%s | dry_run=%s",
                        feature_name(feat), rule.name, possible, cfg["dry_run"])
            LOG.warning("Message: %s", msg)

            if cfg["dry_run"]:
                continue

            # REST headers (global + per-feature + per-feature auth)
            rest_headers = build_rest_headers(
                global_headers=rest_cfg.get("headers", {}) or {},
                feature_headers=feature_rest_headers,
                bearer=rest_bearer,
                basic_user=rest_basic_user,
                basic_pass=rest_basic_pass,
            )

            # MQTT options resolved (feature overrides config)
            mqtt_client_id = feat_mqtt_client_id or mqtt_cfg.get("client_id")
            mqtt_username = feat_mqtt_username or mqtt_cfg.get("username")
            mqtt_password = feat_mqtt_password or mqtt_cfg.get("password")
            mqtt_tls_ca = feat_mqtt_tls_ca or mqtt_cfg.get("tls_ca")
            mqtt_tls_cert = feat_mqtt_tls_cert or mqtt_cfg.get("tls_cert")
            mqtt_tls_key = feat_mqtt_tls_key or mqtt_cfg.get("tls_key")
            mqtt_tls_insecure = (
                feat_mqtt_tls_insecure if feat_mqtt_tls_insecure is not None else bool(mqtt_cfg.get("tls_insecure", False))
            )

            for ch in possible:
                try:
                    if ch == "email":
                        send_email_smtp(
                            smtp_host=email_cfg["smtp_host"],
                            smtp_port=int(email_cfg.get("smtp_port", 587)),
                            use_tls=bool(email_cfg.get("use_tls", True)),
                            username=email_cfg.get("username"),
                            password=email_cfg.get("password"),
                            from_addr=email_cfg.get("from_addr", "LPERFECT Alerts <alerts@localhost>"),
                            to_addr=email,
                            subject=email_cfg.get("subject", "LPERFECT Alert"),
                            body=msg,
                        )

                    elif ch == "sms":
                        twilio_send_message(
                            account_sid=twilio_cfg["account_sid"],
                            auth_token=twilio_cfg["auth_token"],
                            from_number=twilio_cfg["from_sms"],
                            to_number=phone,
                            body=msg,
                        )

                    elif ch == "whatsapp":
                        to = phone if phone.startswith("whatsapp:") else f"whatsapp:{phone}"
                        twilio_send_message(
                            account_sid=twilio_cfg["account_sid"],
                            auth_token=twilio_cfg["auth_token"],
                            from_number=twilio_cfg["from_whatsapp"],
                            to_number=to,
                            body=msg,
                        )

                    elif ch == "telegram":
                        telegram_send_message(
                            bot_token=telegram_cfg["bot_token"],
                            chat_id=tg_chat,
                            text=msg,
                        )

                    elif ch == "rest":
                        rest_notify(
                            url=rest_url,
                            payload=payload,
                            method=rest_cfg.get("method", "POST"),
                            headers=rest_headers,
                        )

                    elif ch == "websocket":
                        websocket_notify(
                            ws_url=ws_url,
                            message=json.dumps(payload, ensure_ascii=False),
                            timeout=int(ws_cfg.get("timeout", 10)),
                        )

                    elif ch == "mqtt":
                        mqtt_publish(
                            mqtt_url=mqtt_url,
                            topic=mqtt_topic,
                            payload=json.dumps(payload, ensure_ascii=False),
                            qos=int(mqtt_cfg.get("qos", 0)),
                            retain=bool(mqtt_cfg.get("retain", False)),
                            client_id=mqtt_client_id,
                            username=mqtt_username,
                            password=mqtt_password,
                            tls_ca=mqtt_tls_ca,
                            tls_cert=mqtt_tls_cert,
                            tls_key=mqtt_tls_key,
                            tls_insecure=bool(mqtt_tls_insecure),
                            timeout=int(mqtt_cfg.get("timeout", 10)),
                        )

                    else:
                        LOG.error("Unknown channel: %s", ch)
                        continue

                    delivered += 1
                    LOG.info("Delivered via %s for feature=%s", ch, feature_name(feat))

                except Exception as e:
                    LOG.error("Delivery failed via %s for %s: %s", ch, feature_name(feat), e)

            if dedupe_enabled:
                sent_map[alert_id] = {
                    "ts": time.time(),
                    "feature": feature_name(feat),
                    "rule": rule.name,
                    "prop": rule.prop,
                    "actual": actual,
                    "channels": possible,
                }

    if dedupe_enabled:
        save_dedupe(dedupe_path, dedupe_state)

    LOG.info("Matched alerts: %d", matched)
    LOG.info("Delivered messages: %d", delivered)
    LOG.info("Skipped by dedupe: %d", skipped_dedupe)
    LOG.info("Missing route/contact/provider: %d", missing_route)
    LOG.info("Dry run: %s", cfg["dry_run"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
