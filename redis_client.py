import socket

class RedisClient():
    def __init__(self, host, port, auth):
        self.host = host
        self.port = port
        self.auth = auth

    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))

        self.file = self.socket.makefile('r')

        if self.auth is not None:
            self.send('auth %s' % (self.auth))

            self.read_response()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        if self.socket:
            self.socket.close()

    def send(self, message):
        return self.socket.sendall("%s\r\n" % message)

    def read_response(self):
        first_line = self.file.readline()
        if first_line.startswith('-'):
            raise RedisError(first_line)

        if first_line.startswith('*'):
            return self.read_array(first_line)
        elif first_line.startswith('$'):
            return self.read_bulk_string(first_line)
        elif first_line.startswith(':'):
            return first_line.lstrip(':').rstrip()
        elif first_line.startswith('+'):
            return first_line.lstrip('+').rstrip()
        else:
            raise ValueError("Unknown Redis response: %s" % first_line)

    def read_array(self, first_line):
        size = int(first_line.lstrip("*").rstrip())
        return [self.read_response() for i in range(0, size)]

    def read_bulk_string(self, first_line):
        size = int(first_line.lstrip("$").rstrip())
        if size == -1:
            return None

        s = self.file.read(size)
        # Get rid of \r\n at end
        self.file.read(2)

        return s


class RedisError(Exception):
    pass
