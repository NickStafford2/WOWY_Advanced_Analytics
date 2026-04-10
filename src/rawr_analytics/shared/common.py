from collections.abc import Callable

type JSONScalar = None | bool | int | float | str
type JSONValue = JSONScalar | list[JSONValue] | dict[str, JSONValue]
type JSONDict = dict[str, JSONValue]

LogFn = Callable[[str], None]
ProgressFn = Callable[[dict], None]
