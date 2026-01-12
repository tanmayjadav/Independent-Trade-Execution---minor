# reporting/discord.py

import requests
from typing import Dict, Optional


class DiscordAlert:
    """
    Sends alerts to Discord via webhook.
    """

    def send_alert(
        self,
        webhook_url: str,
        message: Dict,
        use_embed: bool = False
    ):
        """
        Sends message to Discord webhook.
        
        Args:
            webhook_url: Discord webhook URL
            message: Message dict with title, fields, etc.
            use_embed: If True, sends as embed format
        """
        if not webhook_url:
            return

        try:
            if use_embed:
                embed = {
                    "title": message.get("title", "Alert"),
                    "color": self._get_color_code(message.get("color", "blue")),
                    "fields": [],
                    "timestamp": message.get("date", "")
                }

                # Add all message fields except title and color
                for key, value in message.items():
                    if key not in ("title", "color", "date"):
                        embed["fields"].append({
                            "name": key.replace("_", " ").title(),
                            "value": str(value),
                            "inline": True
                        })

                payload = {"embeds": [embed]}
            else:
                # Simple text message
                text = message.get("title", "")
                if "date" in message:
                    text += f"\n{message['date']}"
                for key, value in message.items():
                    if key not in ("title", "date"):
                        text += f"\n{key}: {value}"

                payload = {"content": text}

            response = requests.post(webhook_url, json=payload, timeout=5)
            response.raise_for_status()

        except Exception as e:
            print(f"Failed to send Discord alert: {e}")

    def _get_color_code(self, color: str) -> int:
        """
        Converts color name to Discord embed color code.
        """
        colors = {
            "green": 0x00ff00,
            "red": 0xff0000,
            "blue": 0x0099ff,
            "yellow": 0xffaa00,
            "orange": 0xff5500,
        }
        return colors.get(color.lower(), 0x0099ff)

