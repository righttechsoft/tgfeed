"""Cerebras AI provider for content summarization."""

import logging
import re
import time

import requests

from .base import AIProvider, SYSTEM_PROMPT

logger = logging.getLogger(__name__)

# Regex to strip thinking tags from Qwen models
THINKING_PATTERN = re.compile(r'<think>.*?</think>\s*', re.DOTALL)


class CerebrasProvider(AIProvider):
    """Cerebras API provider for generating content summaries.

    Cerebras provides fast inference for open-source models like Llama.
    API documentation: https://inference-docs.cerebras.ai/
    """

    def __init__(self, api_key: str, model: str = "llama-3.3-70b"):
        """Initialize the Cerebras provider.

        Args:
            api_key: Cerebras API key
            model: Model to use (default: llama-3.3-70b)
                   Available models: llama-3.3-70b, llama-3.1-8b, llama-3.1-70b
        """
        self.api_key = api_key
        self.model = model
        self.api_url = "https://api.cerebras.ai/v1/chat/completions"

    @property
    def name(self) -> str:
        return "Cerebras"

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def generate_summary(self, message_text: str, max_retries: int = 3) -> str | None:
        """Generate keywords from message text using Cerebras API.

        Handles rate limit errors with exponential backoff.
        """
        if not self.api_key:
            logger.error("Cerebras API key not configured")
            return None

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        payload = {
            "model": self.model,
            "max_tokens": 100,
            "temperature": 0.0,  # Deterministic output for consistency
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": message_text}
            ],
        }

        for attempt in range(max_retries):
            try:
                response = requests.post(
                    self.api_url, headers=headers, json=payload, timeout=30
                )

                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            wait_time = int(retry_after)
                        except ValueError:
                            wait_time = 2 ** attempt
                    else:
                        wait_time = 2 ** attempt  # 1, 2, 4 seconds

                    # Cap wait time at 60 seconds - if API wants longer, fail and retry next run
                    if wait_time > 60:
                        logger.warning(
                            f"Rate limited (429), Retry-After={wait_time}s is too long, skipping"
                        )
                        return None

                    logger.warning(
                        f"Rate limited (429), waiting {wait_time}s before retry "
                        f"{attempt + 1}/{max_retries}"
                    )
                    time.sleep(wait_time)
                    continue

                response.raise_for_status()
                result = response.json()
                content = result["choices"][0]["message"]["content"].strip()
                # Strip thinking tags from Qwen models
                content = THINKING_PATTERN.sub('', content).strip()
                return content

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1 and "429" in str(e):
                    wait_time = 2 ** attempt
                    logger.warning(
                        f"Rate limited, waiting {wait_time}s before retry "
                        f"{attempt + 1}/{max_retries}"
                    )
                    time.sleep(wait_time)
                    continue
                logger.error(f"Cerebras API error: {e}")
                return None
            except (KeyError, IndexError) as e:
                logger.error(f"Unexpected API response format: {e}")
                return None

        logger.error(f"Cerebras API failed after {max_retries} retries (rate limited)")
        return None
