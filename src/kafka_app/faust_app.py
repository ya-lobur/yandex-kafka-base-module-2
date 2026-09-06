"""Faust-приложение: таблицы состояния и потоковая обработка сообщений."""

import logging

import faust

from .censor import censor_text, normalize_word
from .config import (
    APP_ID,
    BANNED_WORDS_PARTITIONS,
    BANNED_WORDS_TOPIC,
    BLOCKED_USERS_TOPIC,
    BROKER_URL,
    FILTERED_MESSAGES_TOPIC,
    MESSAGE_PARTITIONS,
    MESSAGES_TOPIC,
    REPLICATION_FACTOR,
    STORE_URL,
)
from .models import (
    BannedWordState,
    BannedWordUpdate,
    BlockedUsers,
    BlockedUserUpdate,
    ChatMessage,
)

logger = logging.getLogger(__name__)

BLOCK_ACTIONS = {"block", "unblock"}
WORD_ACTIONS = {"ban", "allow"}

app = faust.App(
    APP_ID,
    broker=BROKER_URL,
    store=STORE_URL,
    topic_partitions=MESSAGE_PARTITIONS,
    topic_replication_factor=REPLICATION_FACTOR,
)

messages_topic = app.topic(
    MESSAGES_TOPIC,
    key_type=str,
    value_type=ChatMessage,
)
filtered_messages_topic = app.topic(
    FILTERED_MESSAGES_TOPIC,
    key_type=str,
    value_type=ChatMessage,
)
blocked_users_topic = app.topic(
    BLOCKED_USERS_TOPIC,
    key_type=str,
    value_type=BlockedUserUpdate,
)
banned_words_topic = app.topic(
    BANNED_WORDS_TOPIC,
    key_type=str,
    value_type=BannedWordUpdate,
)

blocked_users = app.Table(
    "blocked-users",
    default=lambda: BlockedUsers(user_ids=[]),
    key_type=str,
    value_type=BlockedUsers,
    partitions=MESSAGE_PARTITIONS,
)
banned_words = app.GlobalTable(
    "banned-words",
    default=lambda: BannedWordState(is_banned=False),
    key_type=str,
    # хотел сделать value_type=bool, но при десериализации выдавало ошибку, тк faust при чтении этой записи из
    # changelog GlobalTable пыталась десериализовать значение через value_type.from_data(...) - такого у bool нет,
    # те подразумевается что value_type должен быть faust-моделью именно
    value_type=BannedWordState,
    partitions=BANNED_WORDS_PARTITIONS,
    recovery_buffer_size=1,
)


@app.agent(blocked_users_topic)
async def update_blocked_users(stream: faust.Stream[BlockedUserUpdate]) -> None:
    """Применять команды block/unblock к таблице пользователя."""

    async for user_id, update in stream.items():
        if user_id != update.user_id:
            logger.warning(
                "Пропущено изменение блокировки: key=%r, user_id=%r",
                user_id,
                update.user_id,
            )
            continue
        if update.action not in BLOCK_ACTIONS:
            logger.warning("Неизвестное действие блокировки: %r", update.action)
            continue

        current = set(blocked_users[user_id].user_ids)
        if update.action == "block":
            current.add(update.blocked_user_id)
        else:
            current.discard(update.blocked_user_id)

        blocked_users[user_id] = BlockedUsers(user_ids=sorted(current))
        logger.info(
            "Список блокировок обновлён: user_id=%s, blocked=%s",
            user_id,
            sorted(current),
        )


@app.agent(banned_words_topic)
async def update_banned_words(stream: faust.Stream[BannedWordUpdate]) -> None:
    """Применять команды ban/allow к глобальной таблице слов."""

    async for word_key, update in stream.items():
        word = normalize_word(update.word)
        if not word or word_key != word:
            logger.warning(
                "Пропущено изменение словаря: key=%r, normalized_word=%r",
                word_key,
                word,
            )
            continue
        if update.action not in WORD_ACTIONS:
            logger.warning("Неизвестное действие словаря: %r", update.action)
            continue

        banned_words[word] = BannedWordState(is_banned=update.action == "ban")
        logger.info(
            "Словарь цензуры обновлён: word=%r, banned=%s",
            word,
            banned_words[word].is_banned,
        )


@app.agent(messages_topic)
async def filter_messages(stream: faust.Stream[ChatMessage]) -> None:
    """Отбрасывать заблокированные сообщения и маскировать остальные."""

    async for recipient_id, message in stream.items():
        if recipient_id != message.recipient_id:
            logger.warning(
                "Пропущено сообщение: key=%r, recipient_id=%r",
                recipient_id,
                message.recipient_id,
            )
            continue

        recipient_blocked_users = blocked_users[recipient_id].user_ids
        if message.user_id in recipient_blocked_users:
            logger.info(
                "Сообщение заблокировано: sender=%s, recipient=%s",
                message.user_id,
                recipient_id,
            )
            continue

        active_banned_words = [word for word, state in banned_words.items() if state.is_banned]
        filtered_message = ChatMessage(
            user_id=message.user_id,
            recipient_id=recipient_id,
            message=censor_text(message.message, active_banned_words),
            timestamp=message.timestamp,
        )
        await filtered_messages_topic.send(
            key=recipient_id,
            value=filtered_message,
            timestamp=message.timestamp / 1000,
        )
        logger.info(
            "Сообщение обработано: sender=%s, recipient=%s",
            message.user_id,
            recipient_id,
        )
