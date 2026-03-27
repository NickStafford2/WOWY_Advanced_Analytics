from typing import Callable

LogFn = Callable[[str], None]
ProgressFn = Callable[[dict], None]
