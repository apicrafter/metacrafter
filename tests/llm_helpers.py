"""Helpers for LLM tests: fake OpenAI-compatible chat completion responses."""
from unittest.mock import Mock

OPENAI_COMPATIBLE_POST = "metacrafter.classify.llm.providers.openai_compatible.requests.post"


def mock_chat(mock_post, content: str) -> None:
    """Make a patched ``requests.post`` return a chat completion with ``content``."""
    response = Mock()
    response.status_code = 200
    response.raise_for_status.return_value = None
    response.json.return_value = {"choices": [{"message": {"content": content}}]}
    mock_post.return_value = response
