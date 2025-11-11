import selectors
from contextlib import suppress
from paho.mqtt.client import Client
from paho.mqtt.enums import MQTTErrorCode

class MQTTClient(Client): 
    
    def _loop(self, timeout: float = 1.0) -> MQTTErrorCode:
        if timeout < 0.0:
            raise ValueError("Invalid timeout.")

        sel = selectors.DefaultSelector()

        eventmask = selectors.EVENT_READ

        with suppress(IndexError):
            packet = self._out_packet.popleft()
            self._out_packet.appendleft(packet)
            eventmask = selectors.EVENT_WRITE | eventmask

        if self._sockpairR is None:
            sel.register(self._sock, eventmask)
        else:
            sel.register(self._sock, eventmask)
            sel.register(self._sockpairR, selectors.EVENT_READ)

        pending_bytes = 0
        if hasattr(self._sock, "pending"):
            pending_bytes = self._sock.pending()

        if pending_bytes > 0:
            timeout = 0.0

        try:
            events = sel.select(timeout)

        except TypeError:
            return MQTTErrorCode.MQTT_ERR_CONN_LOST
        except ValueError:
            return MQTTErrorCode.MQTT_ERR_CONN_LOST
        except Exception:
            return MQTTErrorCode.MQTT_ERR_UNKNOWN

        socklist: list[list] = [[], []]

        for key, _event in events:
            if key.events & selectors.EVENT_READ:
                socklist[0].append(key.fileobj)

            if key.events & selectors.EVENT_WRITE:
                socklist[1].append(key.fileobj)

        if self._sock in socklist[0] or pending_bytes > 0:
            rc = self.loop_read()
            if rc or self._sock is None:
                return rc

        if self._sockpairR and self._sockpairR in socklist[0]:
            with suppress(BlockingIOError):
                self._sockpairR.recv(10000)

        if self._sock in socklist[1]:
            rc = self.loop_write()
            if rc or self._sock is None:
                return rc

        sel.close()

        return self.loop_misc()