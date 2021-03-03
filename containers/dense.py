# copyright (c) 2021 Jason Forbes

import collections.abc

def count_bits(x:int):
    return int(sum(bool(x & (1 << i)) for i in range(x.bit_length())))

class DenseIntegerSet(collections.abc.MutableSet):
    bit_masks = bytes(0x80 >> i for i in range(8))

    def __init__(self, iterable=None, segment_bytelen=512):
        if count_bits(segment_bytelen) != 1:
            raise ValueError("segment_bytelen must be a power of 2.")
        self.segment_bytelen = segment_bytelen
        self.segment_len = self.segment_bytelen * 8
        # subindex is the index of an individual bit counted from the start of
        # the bytearray
        self.subindex_bit_len = (self.segment_len - 1).bit_length()
        self.subindex_bitmask = self.segment_len - 1
        self.counter_bytelen = (self.segment_len.bit_length() - 1) // 8 + 1

        self.segments = {}
        self.clear()
        if iterable is not None:
            for e in iterable:
                self.add(e)

    def _split_key(self, k):
        return (k >> self.subindex_bit_len, (k & subindex_bitmask) >> 3, k & 7)

    def _join_key(self, seg_i, byte_i, bit_i):
        return (seg_i << self.subindex_bit_len) | (byte_i << 3) | bit_i

    def clear(self):
        self.segments.clear()
        self.size = 0

    def __len__(self):
        return self.size

    def __contains__(self, k):
        seg_i, byte_i, bit_i = self._split_key(k)
        try:
            seg = self.segments[seg_i]
        except KeyError:
            return False
        return bool(seg[byte_i] & self.byte_mask[bit_i])

    def __iter__(self):
        for seg_i, seg in self.segments.items():
            for byte_i, byte_ in zip(range(self.segment_bytelen), seg):
                if byte_:
                    for bit_i, mask in enumerate(self.byte_masks):
                        if byte_ & mask:
                            return self._join_key(seg_i, byte_i, bit_i)

    def _counter_arithmetic(self, seg, v):
        view = memoryview(seg)[self.segment_bytelen:]
        new_v = int.from_bytes(view, 'little') + v
        view[:] = new_v.to_bytes(len(view), 'little')
        return new_v

    def add(self, k):
        seg_i, byte_i, bit_i = self._split_key(k)
        try:
            seg = self.segments[seg_i]
        except KeyError:
            self.segments[seg_i] = seg = \
                bytearray(self.segment_bytelen + self.counter_bytelen)

        if not (seg[byte_i] & self.bit_masks[bit_i]):
            seg[byte_i] |= self.bit_masks[bit_i]
            self._counter_arithmetic(seg, 1)

    def discard(self, k):
        seg_i, byte_i, bit_i = self._split_key(k)
        try:
            seg = self.segments[seg_i]
        except KeyError:
            return
        if seg[byte_i] & self.bit_masks[bit_i]:
            ctr = self._counter_arithmetic(seg, -1)
            if ctr == 0:
                del self.segments[seg_i]
                return
            seg[byte_i] &= ~(self.bit_masks[bit_i])
