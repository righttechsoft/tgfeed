"""Base class for AI providers."""

from abc import ABC, abstractmethod


# System prompt for keyword extraction - shared across all providers
SYSTEM_PROMPT = """Extract 3-7 keywords from this post that identify the core event. Output ONLY lowercase keywords separated by commas, sorted alphabetically.

RULES:
1. Extract: main subject, action verb (base form), object, key numbers, locations
2. Use base verb forms: "kill" not "killed/killing", "attack" not "attacked"
3. Normalize names: "usa" not "united states", "uk" not "britain"
4. Numbers: use digits "44b" not "44 billion", "1000" not "1k"
5. No articles (a/an/the), no adjectives, no adverbs
6. No temporal words (today/yesterday/now)
7. Sort alphabetically
8. Translate everything to English

For ads/promos with no news, respond: ad

Examples:
Input: "BREAKING: Tesla CEO Elon Musk announced buying Twitter for $44 billion!"
Output: 44b, acquire, tesla, twitter

Input: "Massive earthquake in Turkey kills thousands, rescue efforts underway"
Output: earthquake, kill, thousands, turkey

Input: "Russian forces attack Kyiv with drones overnight"
Output: attack, drone, kyiv, russia

Input: "Subscribe for more updates! Like and share!"
Output: ad"""


class AIProvider(ABC):
    """Abstract base class for AI providers."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the provider name."""
        pass

    @abstractmethod
    def is_configured(self) -> bool:
        """Check if the provider is properly configured (API key set, etc.)."""
        pass

    @abstractmethod
    def generate_summary(self, message_text: str, max_retries: int = 3) -> str | None:
        """Generate a normalized summary/keywords from the message text.

        Args:
            message_text: The message text to summarize
            max_retries: Maximum number of retries on transient errors

        Returns:
            The extracted keywords as a comma-separated string, or None on error.
        """
        pass
