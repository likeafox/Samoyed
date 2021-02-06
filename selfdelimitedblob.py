__all__ = ["IO","SequentialStorage","MemoryOnlyStorage"]

import io

class IO:
    def __init__(self, stream:io.BufferedIOBase):
        if not isinstance(stream, io.BufferedIOBase):
            raise TypeError("need BufferedIOBase")
        self.stream = stream
        assert stream.isatty() is False and stream.seekable() is True

    @staticmethod
    def read_next(**opts):
        """Read and/or seek through the next blob in the stream, to produce an
        appropriate return value. Return non-None if blob exists, or None if it
        is deleted. The application can optionally provide arbitrary keyword
        arguments to control how the function should read or interpret the
        blob. It is expected that the default behaviour is to simply read the
        entire blob and either return it verbatim, or a translation to its
        associated object."""
        raise NotImplementedError()
    @staticmethod
    def skip_next():
        """Skip the stream ahead to the end of the next blob. Return non-None
        if blob exists, or None if it is deleted."""
        raise NotImplementedError()
    @staticmethod
    def write_obj(obj):
        """Translate obj to blob (if any translation is necessary), and write
        it to the current stream position."""
        raise NotImplementedError()
    @staticmethod
    def delete_next():
        """Modify the next blob in stream in such a way that it would be
        determined to be deleted by load_next and skip_next, but without
        changing its size in stream. As with all the other io operations,
        the stream cursor must be at the end of the blob that was just
        processed when the function exits."""

class SequentialStorage:
    pass

class MemoryOnlyStorage(SequentialStorage):
    def __init__(self, name:str, io_interface, **options):
        self.name = name
        self.io_interface = io_interface
        self._closed = False
        self.reset()

    def test_closed(self):
        if self._closed:
            raise RuntimeError("Storage is closed and can't perform any operation")

    @property
    def store(self):
        self.test_closed()
        return self._store

    @store.setter
    def store(self, value):
        self.test_closed()
        self._store = value

    def reset(self):
        self.test_closed()
        self.id_ctr = 0 # first id will be 1
        self.store = {}

    def append(self, obj):
        self.id_ctr += 1
        s = io.BytesIO()
        self.io_interface(s).write_obj(obj)
        self.store[self.id_ctr] = s.getvalue()
        return self.id_ctr

    def read(self, id, **opts):
        s = io.BytesIO(self.store[id])
        return self.io_interface(s).read_next(**opts)

    def multi_read_iter(self, start_id=0, end_id=float('inf'), **opts):
        ids = (k for k in self.store.keys() if (start_id <= k < end_id))
        for id in ids:
            r = self.read(id, opts=opts)
            if r is not None:
                yield (id, r)

    def discard(self, id):
        del self.store[id]

    def discard_all_before(self, id):
        self.store = dict((k,v) for k,v in self.store.items() if k >= id)

    def close(self):
        del self._store
        self._closed = True
