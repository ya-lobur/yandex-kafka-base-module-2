"""Typer CLI для worker-а и отправки учебных событий."""

import asyncio
from collections.abc import Coroutine
from typing import Any

import faust
import typer

from .faust_app import app as faust_app
from .producer import change_banned_word, change_blocked_user, send_message

app = typer.Typer(
    add_completion=True,
    no_args_is_help=True,
    help="Сервис потоковой фильтрации сообщений с Apache Kafka и Faust.",
)


def _run(coroutine: Coroutine[Any, Any, Any]) -> Any:
    """Запустить асинхронную CLI-команду."""

    try:
        return asyncio.run(coroutine)
    except KeyboardInterrupt:
        typer.echo("Остановка по Ctrl+C")
        raise typer.Exit(code=130) from None


def _normalized_identifier(value: str, parameter: str) -> str:
    """Проверить и нормализовать идентификатор CLI."""

    normalized = value.strip()
    if not normalized:
        raise typer.BadParameter("значение не может быть пустым", param_hint=parameter)
    return normalized


@app.command()
def worker(
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Уровень логирования Faust worker-а.",
    ),
) -> None:
    """Запустить Faust worker и обрабатывать потоки до Ctrl+C."""

    faust.Worker(faust_app, loglevel=log_level.upper()).execute_from_commandline()


@app.command("send-message")
def send_message_command(
    user_id: str = typer.Argument(help="Идентификатор отправителя."),
    recipient_id: str = typer.Argument(help="Идентификатор получателя."),
    message: str = typer.Argument(help="Текст сообщения."),
    timestamp: int | None = typer.Option(
        None,
        min=1,
        help="Unix timestamp в миллисекундах; по умолчанию текущее время.",
    ),
) -> None:
    """Отправить сообщение в топик messages."""

    sender = _normalized_identifier(user_id, "user_id")
    recipient = _normalized_identifier(recipient_id, "recipient_id")
    if not message.strip():
        raise typer.BadParameter("сообщение не может быть пустым", param_hint="message")

    event = _run(
        send_message(
            user_id=sender,
            recipient_id=recipient,
            message=message,
            timestamp_ms=timestamp,
        )
    )
    typer.echo(
        f"Сообщение отправлено: sender={event.user_id}, recipient={event.recipient_id}, timestamp={event.timestamp}"
    )


def _change_block(user_id: str, blocked_user_id: str, action: str) -> None:
    owner = _normalized_identifier(user_id, "user_id")
    blocked = _normalized_identifier(blocked_user_id, "blocked_user_id")
    update = _run(
        change_blocked_user(
            user_id=owner,
            blocked_user_id=blocked,
            action=action,
        )
    )
    typer.echo(
        f"Команда отправлена: user={update.user_id}, blocked_user={update.blocked_user_id}, action={update.action}"
    )


@app.command("block-user")
def block_user(
    user_id: str = typer.Argument(help="Владелец списка блокировок."),
    blocked_user_id: str = typer.Argument(help="Блокируемый отправитель."),
) -> None:
    """Добавить отправителя в список блокировок пользователя."""

    _change_block(user_id, blocked_user_id, "block")


@app.command("unblock-user")
def unblock_user(
    user_id: str = typer.Argument(help="Владелец списка блокировок."),
    blocked_user_id: str = typer.Argument(help="Разблокируемый отправитель."),
) -> None:
    """Удалить отправителя из списка блокировок пользователя."""

    _change_block(user_id, blocked_user_id, "unblock")


def _change_word(word: str, action: str) -> None:
    normalized_word = word.strip()
    if not normalized_word:
        raise typer.BadParameter("слово не может быть пустым", param_hint="word")
    update = _run(change_banned_word(word=normalized_word, action=action))
    typer.echo(f"Команда отправлена: word={update.word!r}, action={update.action}")


@app.command("ban-word")
def ban_word(word: str = typer.Argument(help="Запрещаемое слово или фраза.")) -> None:
    """Добавить слово в глобальный список цензуры."""

    _change_word(word, "ban")


@app.command("allow-word")
def allow_word(word: str = typer.Argument(help="Разрешаемое слово или фраза.")) -> None:
    """Удалить слово из глобального списка цензуры."""

    _change_word(word, "allow")


def main() -> None:
    """Точка входа команды kafka-app."""

    app()


if __name__ == "__main__":
    main()
