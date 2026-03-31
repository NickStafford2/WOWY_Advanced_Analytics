from collections.abc import Callable

LogFn = Callable[[str], None]
ProgressFn = Callable[[dict], None]
