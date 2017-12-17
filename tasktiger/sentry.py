# Make "import rollbar" import rollbar, and not the current module.
from __future__ import absolute_import

import json
from raven.handlers.logging import SentryHandler


class StructlogSentryHandler(SentryHandler):
    def __init__(self, prefix, *args, **kwargs):
        """
        Structured sentry handler. Sentry messages are prefixed with the
        given prefix string. Any other arguments are passed to SentryHandler.
        """
        self.prefix = prefix
        super(StructlogSentryHandler, self).__init__(*args, **kwargs)

    def format_title(self, data):
        # Keys used to construct the title and for grouping purposes.
        KEYS = ['event', 'func', 'exception_name', 'queue']

        def format_field(field, value):
            if field == 'queue':
                return '{}={}'.format(field, value.split('.')[0])
            else:
                return '{}={}'.format(field, value)
        return '{}: {}'.format(self.prefix, ' '.join(format_field(key, data[key]) for key in KEYS if key in data))

    def emit(self, record):
        try:
            data = json.loads(record.msg)
        except json.JSONDecodeError:
            return super(StructlogSentryHandler, self).emit(record)

        # Title and grouping
        data['title'] = data['fingerprint'] = self.format_title(data)
        record.msg = json.dumps(data)
        super().emit(record)
