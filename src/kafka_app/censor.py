"""Маскирование запрещённых слов в тексте сообщений."""

import re
from collections.abc import Iterable


def normalize_word(word: str) -> str:
    """Нормализовать слово для хранения и поиска без учёта регистра."""

    return word.strip().casefold()


def censor_text(text: str, banned_words: Iterable[str]) -> str:
    """Заменить целые запрещённые слова звёздочками той же длины."""

    normalized_words = {normalized for word in banned_words if (normalized := normalize_word(word))}
    if not normalized_words:
        return text

    alternatives = "|".join(re.escape(word) for word in sorted(normalized_words, key=len, reverse=True))
    pattern = re.compile(rf"(?<!\w)(?:{alternatives})(?!\w)", re.IGNORECASE)
    return pattern.sub(lambda match: "*" * len(match.group(0)), text)
