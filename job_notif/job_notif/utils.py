def truncate_text(text: str, max_char: int = 500) -> str:
    if not text:
        return ""

    if len(text) > max_char:
        text = text[:max_char] + "..."

    return text
