"""Асинхронная отправка тестовых событий в Kafka через Faust producer."""

from datetime import UTC, datetime

from .censor import normalize_word
from .faust_app import (
    app,
    banned_words_topic,
    blocked_users_topic,
    messages_topic,
)
from .models import BannedWordUpdate, BlockedUserUpdate, ChatMessage


def current_timestamp_ms() -> int:
    """Вернуть текущее время в Unix milliseconds."""

    return int(datetime.now(UTC).timestamp() * 1000)


async def _send(*, topic, key: str, value, timestamp: float | None = None) -> None:
    """Отправить одно событие и закрыть producer конечной CLI-команды."""

    try:
        await topic.send(key=key, value=value, timestamp=timestamp)
    finally:
        await app.producer.stop()


async def send_message(
    *,
    user_id: str,
    recipient_id: str,
    message: str,
    timestamp_ms: int | None = None,
) -> ChatMessage:
    """Опубликовать сообщение с ключом получателя."""

    event_timestamp_ms = timestamp_ms or current_timestamp_ms()
    event = ChatMessage(
        user_id=user_id,
        recipient_id=recipient_id,
        message=message,
        timestamp=event_timestamp_ms,
    )
    await _send(
        topic=messages_topic,
        key=recipient_id,
        value=event,
        timestamp=event_timestamp_ms / 1000,
    )
    return event


async def change_blocked_user(
    *,
    user_id: str,
    blocked_user_id: str,
    action: str,
) -> BlockedUserUpdate:
    """Опубликовать команду block или unblock."""

    update = BlockedUserUpdate(
        user_id=user_id,
        blocked_user_id=blocked_user_id,
        action=action,
    )
    await _send(topic=blocked_users_topic, key=user_id, value=update)
    return update


async def change_banned_word(*, word: str, action: str) -> BannedWordUpdate:
    """Опубликовать команду ban или allow."""

    normalized_word = normalize_word(word)
    update = BannedWordUpdate(word=normalized_word, action=action)
    await _send(topic=banned_words_topic, key=normalized_word, value=update)
    return update
