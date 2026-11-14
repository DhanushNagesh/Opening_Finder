from dataclasses import dataclass, field
from datetime import date, time


@dataclass(frozen=True, slots=True)
class Game:
    game_id: str
    event: str
    site: str
    utc_date: date
    utc_time: time
    white: str
    black: str
    result: str
    white_elo: int | None
    black_elo: int | None
    white_rating_diff: int | None
    black_rating_diff: int | None
    eco: str | None
    opening: str | None
    time_control: str
    base_seconds: int | None
    increment_seconds: int | None
    termination: str
    movetext: str
    ply_count: int
    has_clock: bool
    has_eval: bool


@dataclass
class ParseStats:
    games_ok: int = 0
    games_malformed: int = 0
    lines_read: int = 0
    stray_lines: int = 0
    malformed_reasons: dict[str, int] = field(default_factory=dict)
    # (line number where the game starts, reason), capped so a broken file can't eat memory
    malformed_examples: list[tuple[int, str]] = field(default_factory=list)

    MAX_EXAMPLES = 100

    def record_malformed(self, start_line: int, reason: str) -> None:
        self.games_malformed += 1
        key = reason.split(":", 1)[0]
        self.malformed_reasons[key] = self.malformed_reasons.get(key, 0) + 1
        if len(self.malformed_examples) < self.MAX_EXAMPLES:
            self.malformed_examples.append((start_line, reason))
