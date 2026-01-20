# reporting/discord.py

import requests
from typing import Dict, Optional
from datetime import datetime


class DiscordAlert:
    """
    Simple Discord webhook alert sender.
    Use different webhook URLs for different channels (trades, alerts, etc.)
    """

    def send_alert(
        self,
        webhook_url: str,
        message: Dict,
        use_embed: bool = True
    ):
        """
        Sends message to Discord webhook.
        
        Args:
            webhook_url: Discord webhook URL (use different URLs for different channels)
            message: Message dict with:
                - title: Alert title
                - description: Optional description text
                - color: Optional color ("red", "green", "blue", "yellow", "orange", "purple")
                - fields: Optional list of field dicts with "name", "value", "inline" keys
                - Or any other keys will be added as fields automatically
            use_embed: If True, sends as embed format (default: True)
        """
        if not webhook_url:
            return

        try:
            if use_embed:
                embed = {
                    "title": message.get("title", "Alert"),
                    "color": self._get_color_code(message.get("color", "blue")),
                    "timestamp": datetime.utcnow().isoformat()
                }

                # Add description if provided
                if "description" in message:
                    desc = str(message["description"])
                    if len(desc) > 2048:
                        desc = desc[:2045] + "..."
                    embed["description"] = desc

                # Add fields if provided as list
                if "fields" in message and isinstance(message["fields"], list):
                    embed["fields"] = message["fields"]
                else:
                    # Auto-create fields from other keys
                    embed["fields"] = []
                    for key, value in message.items():
                        if key not in ("title", "color", "description", "fields", "date"):
                            value_str = str(value)
                            if len(value_str) > 1024:
                                value_str = value_str[:1021] + "..."
                            embed["fields"].append({
                                "name": key.replace("_", " ").title(),
                                "value": value_str,
                                "inline": True
                            })

                payload = {"embeds": [embed]}
            else:
                # Simple text message
                text = message.get("title", "")
                if "description" in message:
                    text += f"\n{message['description']}"
                for key, value in message.items():
                    if key not in ("title", "description"):
                        text += f"\n{key}: {value}"

                payload = {"content": text[:2000]}  # Discord limit is 2000 chars

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
            "purple": 0x9932cc,
        }
        return colors.get(color.lower(), 0x0099ff)

