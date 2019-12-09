from kombu.transport import virtual
from kombu.transport import redis
from kombu.transport import TRANSPORT_ALIASES
from kombu.utils.encoding import bytes_to_str


class Channel(redis.Channel):
    ack_emulation = False
    supports_fanout = True

    def _size(self, queue):
        size = 0
        with self.conn_or_acquire() as client:
            for pri in self.priority_steps:
                size += client.llen(self._q_for_pri(queue, pri))
        return size

    def _purge(self, queue):
        size = 0
        with self.conn_or_acquire() as client:
            for pri in self.priority_steps:
                priq = self._q_for_pri(queue, pri)
                size += client.llen(priq)
                client.delete(priq)
        return size

    def _delete(self, queue, exchange, routing_key, pattern, *args, **kwargs):
        self.auto_delete_queues.discard(queue)
        with self.conn_or_acquire(client=kwargs.get('client')) as client:
            client.srem(self.keyprefix_queue % (exchange,),
                        self.sep.join([routing_key or '',
                                       pattern or '',
                                       queue or '']))
            for pri in self.priority_steps:
                client.delete(self._q_for_pri(queue, pri))

    def _has_queue(self, queue, **kwargs):
        with self.conn_or_acquire() as client:
            for pri in self.priority_steps:
                if client.exists(self._q_for_pri(queue, pri)):
                    return True
        return False


class Transport(redis.Transport):
    """Nutcracker Transport."""

    driver_type = 'nutcracker'
    driver_name = 'nutcracker'
    Channel = Channel
    implements = virtual.Transport.implements.extend(
        asynchronous=True,
        exchange_type=frozenset(['direct', 'topic'])
    )


def register():
    TRANSPORT_ALIASES[Transport.driver_type] = "%s:%s" % (
        Transport.__module__, Transport.__name__)


def unregister():
    TRANSPORT_ALIASES.pop(Transport.driver_type, None)
