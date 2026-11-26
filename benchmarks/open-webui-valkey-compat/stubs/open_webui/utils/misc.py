"""Stand-in for open_webui.utils.misc.sanitize_text_for_db: strip NULs, keep other values."""


def sanitize_text_for_db(text):
    if isinstance(text, str):
        return text.replace("\x00", "")
    return text
