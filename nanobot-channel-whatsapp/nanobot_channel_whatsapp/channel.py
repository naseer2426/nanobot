import asyncio
from typing import Any

from aiohttp import ClientSession, ClientTimeout, web
from loguru import logger
from pydantic import Field

from nanobot.channels.base import BaseChannel
from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.config.schema import Base


class WhatsAppConfig(Base):
    """WhatsApp channel configuration (HTTP ingress for Meta-style webhooks)."""

    enabled: bool = False
    port: int = 9000
    allow_from: list[str] = Field(default_factory=list)
    whatsapp_api_url: str = ""


class WhatsAppChannel(BaseChannel):
    name = "whatsapp"
    display_name = "WhatsApp"

    def __init__(self, config: Any, bus: MessageBus):
        if isinstance(config, dict):
            config = WhatsAppConfig(**config)
        super().__init__(config, bus)

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        return WhatsAppConfig().model_dump(by_alias=True)

    async def start(self) -> None:
        """Start an HTTP server that listens for incoming messages.

        IMPORTANT: start() must block forever (or until stop() is called).
        If it returns, the channel is considered dead.
        """
        self._running = True
        port = self.config.port

        app = web.Application()
        app.router.add_post("/message", self._on_request)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        logger.info("WhatsApp channel HTTP ingress listening on :{}", port)

        # Block until stopped
        while self._running:
            await asyncio.sleep(1)

        await runner.cleanup()

    async def stop(self) -> None:
        self._running = False

    async def send(self, msg: OutboundMessage) -> None:
        """Deliver an outbound message.

        msg.content  — markdown text (convert to platform format as needed)
        msg.media    — list of local file paths to attach
        msg.chat_id  — the recipient (same chat_id you passed to _handle_message)
        msg.metadata — may contain "_progress": True for streaming chunks
        """
        base = (self.config.whatsapp_api_url or "").strip().rstrip("/")
        if not base:
            logger.warning("WhatsApp send skipped: whatsapp_api_url is not configured")
            return

        chat_id = (msg.chat_id or "").strip()
        if not chat_id:
            logger.warning("WhatsApp send skipped: empty chat_id")
            return

        text = msg.content or ""
        if not text.strip():
            return

        url = f"{base}/send_message_to_chat"
        payload = {"message": text, "chat_id": chat_id}

        timeout = ClientTimeout(total=120)
        async with ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload) as resp:
                if resp.status >= 400:
                    detail = (await resp.text()).strip()
                    err = f"WhatsApp API HTTP {resp.status} for {url}: {detail}"
                    logger.error(err)
                    raise RuntimeError(err)

    async def _on_request(self, request: web.Request) -> web.Response:
        """Handle an incoming HTTP POST."""
        body = await request.json()
        sender = body.get("sender", "unknown")
        chat_id = body.get("chat_id", sender)
        text = body.get("text", "")
        media = body.get("media", [])  # list of URLs

        await self._handle_message(
            sender_id=sender,
            chat_id=chat_id,
            content=text,
            media=media,
        )

        return web.json_response({"ok": True})
