"""
DDAnalyze — SEMOSS Client Adapter

Wraps SEMOSS's REST API to provide a drop-in replacement for the direct
Anthropic SDK client. When `use_semoss: true` is set in config.yaml, all
LLM calls and data queries are routed through SEMOSS instead of calling
Anthropic directly.

SEMOSS API reference
─────────────────────
  POST  /api/engine/MODEL/{modelEngineId}/ask        ← LLM inference
  POST  /api/engine/DATABASE/{dbEngineId}/sql-query  ← SQL data query
  GET   /api/auth/login/check                        ← session health

Environment variables (or config.yaml):
  SEMOSS_HOST          e.g. http://semoss.internal:8080
  SEMOSS_MODEL_ID      Model engine ID registered in SEMOSS
  SEMOSS_DB_ID         Database engine ID registered in SEMOSS (optional)
  SEMOSS_ACCESS_TOKEN  Bearer token (or leave blank for session auth)
  SEMOSS_USERNAME      Used if token auth is not available
  SEMOSS_PASSWORD      Used if token auth is not available
"""

from __future__ import annotations

import logging
import os
import time
from typing import Optional

import requests

logger = logging.getLogger("ddanalyze.semoss")


class SEMOSSUsage:
    """Minimal usage object to match Anthropic's response.usage interface."""

    def __init__(self, input_tokens: int = 0, output_tokens: int = 0):
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens


class SEMOSSContentBlock:
    """Minimal content block to match Anthropic's response.content interface."""

    def __init__(self, text: str):
        self.type = "text"
        self.text = text


class SEMOSSMessage:
    """Minimal message object to match Anthropic's Message interface."""

    def __init__(self, text: str, input_tokens: int = 0, output_tokens: int = 0):
        self.content = [SEMOSSContentBlock(text)]
        self.usage = SEMOSSUsage(input_tokens, output_tokens)


class SEMOSSStreamContext:
    """
    Minimal context manager that mimics Anthropic's client.messages.stream(...)
    so that existing call_llm* code works without modification.

    Usage (mirrors Anthropic SDK):
        with client.messages.stream(...) as stream:
            response = stream.get_final_message()
    """

    def __init__(self, message: SEMOSSMessage):
        self._message = message

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def get_final_message(self) -> SEMOSSMessage:
        return self._message


class SEMOSSMessages:
    """Proxy object that provides the `.stream()` interface."""

    def __init__(self, client: "SEMOSSClient"):
        self._client = client

    def stream(
        self,
        model: str,
        max_tokens: int,
        system: str,
        messages: list,
    ) -> SEMOSSStreamContext:
        """
        Calls SEMOSS Model Engine and returns a context manager that yields
        a SEMOSSMessage, mirroring the Anthropic streaming interface.
        """
        # Flatten messages into a single user prompt the same way the current
        # code always sends a single user message.
        user_content = ""
        for m in messages:
            if m.get("role") == "user":
                user_content = m.get("content", "")
                break

        t0 = time.time()
        text, in_tok, out_tok = self._client._ask_model(system, user_content, max_tokens)
        elapsed = time.time() - t0
        logger.info(
            f"  [SEMOSS] Model call done in {elapsed:.1f}s "
            f"| in={in_tok} out={out_tok}"
        )
        return SEMOSSStreamContext(SEMOSSMessage(text, in_tok, out_tok))


class SEMOSSClient:
    """
    SEMOSS REST client.

    Mirrors the subset of the Anthropic client interface used by DDAnalyze:
      client.messages.stream(model, max_tokens, system, messages)

    Additional SEMOSS-specific method:
      client.query_database(sql) → pandas.DataFrame
    """

    def __init__(
        self,
        host: str,
        model_engine_id: str,
        db_engine_id: Optional[str] = None,
        access_token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify_ssl: bool = True,
        timeout: int = 120,
    ):
        self.host = host.rstrip("/")
        self.model_engine_id = model_engine_id
        self.db_engine_id = db_engine_id
        self.verify_ssl = verify_ssl
        self.timeout = timeout

        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

        if access_token:
            self._session.headers.update({"Authorization": f"Bearer {access_token}"})
        elif username and password:
            self._login(username, password)

        self.messages = SEMOSSMessages(self)

    # ── Authentication ────────────────────────────────────────────────────────

    def _login(self, username: str, password: str) -> None:
        url = f"{self.host}/api/auth/login"
        resp = self._session.post(
            url,
            json={"username": username, "password": password},
            verify=self.verify_ssl,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        logger.info("[SEMOSS] Authenticated via username/password")

    # ── Model Engine ─────────────────────────────────────────────────────────

    def _ask_model(
        self,
        system_prompt: str,
        user_message: str,
        max_tokens: int = 4096,
    ) -> tuple[str, int, int]:
        """
        POST /api/engine/MODEL/{modelEngineId}/ask

        Returns (response_text, input_tokens, output_tokens).
        Token counts are approximate when SEMOSS does not surface them.
        """
        url = f"{self.host}/api/engine/MODEL/{self.model_engine_id}/ask"
        payload = {
            "command": user_message,
            "context": system_prompt,
            "paramValues": {"max_new_tokens": max_tokens},
        }
        logger.debug(f"[SEMOSS] POST {url} | user_msg_len={len(user_message)}")

        resp = self._session.post(
            url,
            json=payload,
            verify=self.verify_ssl,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        data = resp.json()

        # SEMOSS returns the answer under different keys depending on version
        text = (
            data.get("response")
            or data.get("output")
            or data.get("answer")
            or data.get("message")
            or str(data)
        )

        # Use token counts if the platform provides them
        in_tok = data.get("inputTokens", data.get("input_tokens", 0))
        out_tok = data.get("outputTokens", data.get("output_tokens", 0))

        return text, int(in_tok), int(out_tok)

    # ── Database Engine ───────────────────────────────────────────────────────

    def query_database(self, sql: str):
        """
        POST /api/engine/DATABASE/{dbEngineId}/sql-query

        Returns a pandas DataFrame. Requires `db_engine_id` to be set.
        Raises RuntimeError if no database engine is configured.
        """
        if not self.db_engine_id:
            raise RuntimeError(
                "SEMOSS database engine ID not configured. "
                "Set SEMOSS_DB_ID or semoss_db_id in config.yaml."
            )

        import pandas as pd  # local import — not a hard dependency for LLM-only mode

        url = f"{self.host}/api/engine/DATABASE/{self.db_engine_id}/sql-query"
        payload = {"query": sql}
        logger.debug(f"[SEMOSS] DB query: {sql[:120]}")

        resp = self._session.post(
            url,
            json=payload,
            verify=self.verify_ssl,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        data = resp.json()

        # SEMOSS typically returns { "data": { "values": [...], "headers": [...] } }
        inner = data.get("data", data)
        headers = inner.get("headers", [])
        values = inner.get("values", inner.get("data", []))

        if not headers and isinstance(values, list) and values and isinstance(values[0], dict):
            return pd.DataFrame(values)

        return pd.DataFrame(values, columns=headers) if headers else pd.DataFrame(values)


# ── Factory ───────────────────────────────────────────────────────────────────


def create_semoss_client(config: dict) -> SEMOSSClient:
    """
    Build a SEMOSSClient from config dict + environment variables.

    Config keys (all optional — env vars take precedence):
      semoss_host, semoss_model_id, semoss_db_id,
      semoss_access_token, semoss_username, semoss_password, semoss_verify_ssl
    """
    host = os.getenv("SEMOSS_HOST") or config.get("semoss_host", "")
    model_id = os.getenv("SEMOSS_MODEL_ID") or config.get("semoss_model_id", "")
    db_id = os.getenv("SEMOSS_DB_ID") or config.get("semoss_db_id") or None
    token = os.getenv("SEMOSS_ACCESS_TOKEN") or config.get("semoss_access_token") or None
    username = os.getenv("SEMOSS_USERNAME") or config.get("semoss_username") or None
    password = os.getenv("SEMOSS_PASSWORD") or config.get("semoss_password") or None
    verify_ssl = config.get("semoss_verify_ssl", True)

    if not host:
        raise ValueError(
            "SEMOSS host not configured. "
            "Set SEMOSS_HOST env var or semoss_host in config.yaml."
        )
    if not model_id:
        raise ValueError(
            "SEMOSS model engine ID not configured. "
            "Set SEMOSS_MODEL_ID env var or semoss_model_id in config.yaml."
        )

    client = SEMOSSClient(
        host=host,
        model_engine_id=model_id,
        db_engine_id=db_id,
        access_token=token,
        username=username,
        password=password,
        verify_ssl=verify_ssl,
    )
    logger.info(f"[SEMOSS] Client initialised | host={host} | model={model_id} | db={db_id}")
    return client
