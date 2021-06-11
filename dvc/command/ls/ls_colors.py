import os


class LsColors:
    default = "rs=0:di=01;34:ex=01;32"

    def __init__(self, lscolors=None):
        self._extensions = {}
        self._codes = {}
        self._load(lscolors or os.environ.get("LS_COLORS") or LsColors.default)

    def _load(self, lscolors):
        for item in lscolors.split(":"):
            try:
                code, color = item.split("=", 1)
            except ValueError:
                continue
            if code.startswith("*."):
                self._extensions[code[1:]] = color
            else:
                self._codes[code] = color

    def format(self, entry):
        text = entry["path"]

        if entry.get("isout", False) and "out" in self._codes:
            return self._format(text, code="out")

        if entry.get("isdir", False):
            return self._format(text, code="di")

        if entry.get("isexec", False):
            return self._format(text, code="ex")

        _, ext = os.path.splitext(text)
        return self._format(text, ext=ext)

    def _format(self, text, code=None, ext=None):
        val = None
        if ext:
            val = self._extensions.get(ext, None)
        if code:
            val = self._codes.get(code, None)

        if not val:
            return text
        rs = self._codes.get("rs", 0)
        return f"\033[{val}m{text}\033[{rs}m"
