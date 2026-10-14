"""对完整用户输入做轻量分类，不改写原始文本。"""

from dataclasses import dataclass
from unicodedata import category

DEFAULT_FILLER_CHARS = "嗯哦噢喔啊呃额"


@dataclass(frozen=True, slots=True)
class FillerFilter:
    enabled: bool = True
    chars: str = DEFAULT_FILLER_CHARS
    max_chars: int = 6

    def filtered_char_count(self, text: str) -> int | None:
        """纯语气词返回有效字符数；其余输入（含空白输入）返回 None。"""
        if not self.enabled or "?" in text or "？" in text:
            return None
        count = 0
        for char in text:
            if char.isspace() or category(char).startswith("P"):
                continue
            if char not in self.chars:
                return None
            count += 1
            if count > self.max_chars:
                return None
        return count or None
