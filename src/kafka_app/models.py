"""JSON-модели событий и состояния Faust."""

import faust


class ChatMessage(faust.Record, serializer="json"):
    """Сообщение между двумя пользователями."""

    user_id: str
    recipient_id: str
    message: str
    timestamp: int


class BlockedUserUpdate(faust.Record, serializer="json"):
    """Команда изменения списка заблокированных отправителей."""

    user_id: str
    blocked_user_id: str
    action: str


class BannedWordUpdate(faust.Record, serializer="json"):
    """Команда изменения глобального списка запрещённых слов."""

    word: str
    action: str


class BannedWordState(faust.Record, serializer="json"):
    """Сериализуемое состояние одного слова в глобальной таблице."""

    is_banned: bool


class BlockedUsers(faust.Record, serializer="json"):
    """Сериализуемое состояние блокировок одного пользователя."""

    user_ids: list[str]
