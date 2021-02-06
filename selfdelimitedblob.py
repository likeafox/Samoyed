__all__ = ["IO","SequentialStorage","MemoryOnlyStorage"]

import io

class IO:
    def __init__(self, stream:io.BufferedIOBase=None):
        if stream is None:
            self.stream = io.BytesIO()
        elif not isinstance(stream, io.BufferedIOBase):
            raise TypeError("need BufferedIOBase")
        else:
            self.stream = stream
        assert stream.isatty() is False and stream.seekable() is True

    @staticmethod
    def read_next(opt=None):
        """Read and/or seek through the next blob in the stream, to produce an
        appropriate return value. Return non-None if blob exists, or None if it
        is deleted. The application can optionally provide opt to control how
        the function should read or interpret the blob. It is expected that
        the default behaviour is to simply read the entire blob and return
        it verbatim, or a translation to its associated object."""
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
    def __init__(self, name, sdb_skip_func, est_store_size, max_sdb_size, **options):
        self.reset()

    def close(self):
        pass

    def append(self, blob):
        if type(blob) not in (bytes, bytearray):
            raise TypeError()
        self.ctr += 1
        self.store[self.ctr] = blob
        return self.ctr

    def discard(self, id):
        if id not in self.store:
            raise ValueError()
        del self.store[id]

    def discard_all_before(self, id):
        return dict((k,v) for k,v in self.store.items() if k >= id)

    def get_stream_for(self, id):
        return self.store[id]
        # if id not in self.store:
        #     raise KeyError()

    def reset(self):
        self.ctr = 0 # first id will be 1
        self.store = {}

    def read_ctx(self, start_id=0):
        it_obj = self._it()
        #dict items are iterated in insertion order in py 3.7
        it_obj.store_iter = iter(self.store.items())
        return it_obj

    class _it:
        def __iter__(self):
            return self
        def __next__(self):
            r, self.cur_item = next(self.cache_iter)
            return r
        def get_stream_for_cur_item(self):
            return io.BytesIO(self.cur_item)

# class DefaultLocalCache(LocalCache):
#     def __init__(self, name, est_size, est_msg_size, **options):
#         assert path in options
#         raise NotImplementedError()
        #self.max_items = options.get('max_items', None)
        #if self.max_items is not None and len(self.cache) > self.max_items:
        #    del self.cache[next(iter(self.cache))]
