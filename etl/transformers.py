import bonobo
from bonobo.config import use, ContextProcessor, Configurable, Option
from bonobo.util.objects import ValueHolder
from bonobo.constants import NOT_MODIFIED


class Uniquify(Configurable):
    """Deduplicate rows based on a field"""
    field = Option(positional=True, default=0)

    @ContextProcessor
    def unique_set(self, context):
        yield ValueHolder(set())

    def __call__(self, unique_set, *args, **kwargs):
        if args[self.field] not in unique_set:
            unique_set.add(args[self.field])
            yield NOT_MODIFIED

