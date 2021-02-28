# copyright (c) 2021 Jason Forbes

import random, itertools, operator
from collections import deque, namedtuple
import collections.abc



class Node:
    __slots__ = ('children','k')
    def __init__(self, k):
        self.children = [None, None]
        self.k = k

    def __repr__(self):
        cks = [getattr(c,'k',None) for c in self.children]
        return f"<{__name__}.{self.__class__.__qualname__} ({self.k}) {cks}>"



# global consts
directions = namedtuple('DirectionEnum',"left right")(0,1)
limits = [object(),object()]



class Cursor:
    __slots__ = ('iter_direction', 'path_nodes', 'has_result', 'is_closed')
    def __init__(self, root:Node, iter_direction=directions.right, stack=None):
        self.iter_direction = iter_direction
        self.path_nodes = stack if (stack is not None) else deque()
        self.path_nodes.append(root)
        self.has_result = root is not None
        self.is_closed = root is None

    def close(self):
        self.is_closed = True

    def __bool__(self):
        return self.has_result

    @property
    def node(self):
        return self.path_nodes[-1] if self.has_result else None

    def find(self, k):
        if self.is_closed:
            self.has_result = False
            return self

        if k not in limits: #find specific Node(k)
            n = self.path_nodes[-1]
            while n.k != k:
                n = n.children[int(n.k < k)]
                if n is None:
                    break
                self.path_nodes.append(n)
            self.has_result = n is not None
        else: #go to limit
            course = limits.index(k)
            n = self.path_nodes[-1].children[course]
            while n is not None:
                self.path_nodes.append(n)
                n = n.children[course]
            self.has_result = True
        return self

    def reverse(self):
        self.iter_direction ^= 1

    def __iter__(self):
        return self

    def __next__(self):
        if self.is_closed:
            self.has_result = False
            raise StopIteration()
        def node(): return self.path_nodes[-1]

        course = self.iter_direction
        while True:
            # move
            if not course & -2: #then course is pointing downward
                next_ = node().children[course]
                if next_ is not None: #then node is valid; go
                    self.path_nodes.append(next_)
                    course = [2,-1][self.iter_direction]
            else: #upward course
                prev = self.path_nodes.pop()
                try:
                    course = node().children.index(prev)
                except IndexError:
                    self.close()
                    raise StopIteration()
            # pivot
            course += (self.iter_direction << 1) - 1
            if course == self.iter_direction:
                self.has_result = True
                return node().k



class KeyspaceSlice:
    contains_tests = {
        (0,0,1) : lambda k: True,
        (1,0,1) : lambda k: not k < start,
        (0,1,1) : lambda k: k < stop,
        (1,1,1) : lambda k: (not k < start) and k < stop,
        (0,0,0) : lambda k: True,
        (1,0,0) : lambda k: not start < k,
        (0,1,0) : lambda k: stop < k,
        (1,1,0) : lambda k: (not start < k) and stop < k,
    }
    
    def __init__(self, start, stop, direction):
        self.start = start
        self.stop = stop
        self.direction = direction
        test_selection = (start is not None, stop is not None, direction)
        self.contains_test = self.contains_tests[test_selection]

    def __contains__(self, k):
        return self.contains_test(k)

    def __getitem__(self, slice_):
        if type(slice_) is not slice:
            raise TypeError("This function is for sub-slicing only.")
        if slice_.step is not None:
            raise TypeError("As integral (int) SearchTree keys are not "\
                            "enforced, slice steps cannot be supported.")
        start = slice_.start if (slice_.start in self) else self.start
        stop = slice_.stop if (slice_.stop in self) else self.stop
        return self.__class__(start, stop, self.direction)



class SearchTree:
    class AnchorK:
        def __lt__(self, other):
            return True

    def __init__(self, default_iter_direction=directions.right):
        self.default_iter_direction = default_iter_direction
        self.clear()

    def clear(self):
        self.anchor = Node(self.AnchorK())
        self.size = 0
        self._clear_last_found()

    def insert_or_replace(self, node_or_k):
        new_node = node_or_k if isinstance(node_or_k,Node) else Node(node_or_k)
        k = new_node.k
        find_existing = Cursor(self.anchor, stack=[]).find(k)
        path = find_existing.path_nodes

        if find_existing: # then k is already in tree; replace its node
            parent = path[-2]
            course = parent.children.index(find_existing.node)
            parent.children[course] = new_node
            new_node.children = find_existing.node.children
            return

        drop_depth = random.getrandbits(self.size.bit_length()).bit_length() if self.size else 0
        if drop_depth + 1 >= len(path):
            path[-1].children[int(path[-1].k < k)] = new_node
        else:
            parent = path[drop_depth]
            course = int(parent.k < k)
            node = parent.children[course]
            parent.children[course] = node
            course = int(k < node.k)
            new_node.children[course] = node

            #took another node's place; rebase descendant nodes as needed
            rebase_dest = new_node
            course ^= 1
            while True:
                child = node.children[course]
                if child is None:
                    break
                elif int(child.k < k) ^ course:
                    # need to rebase this one
                    assert rebase_dest.children[int(rebase_dest.k < child.k)] is None
                    rebase_dest.children[int(rebase_dest.k < child.k)] = child
                    rebase_dest = node
                    node.children[course] = None
                    course ^= 1
                node = child
        self.size += 1

    def delete(self, k):
        if self.last_found.k == k:
            self._clear_last_found()
        find_del = Cursor(self.anchor, stack=deque(maxlen=2)).find(k)
        if not find_del:
            raise LookupError(k)
        del_node_parent, del_node = find_del.path_nodes
        del_course = del_node_parent.children.index(del_node)
        del_children = [n for n in del_node.children if n is not None]
        if len(del_children) == 2:
            #find a suitable replacement node
            course = random.getrandbits(1)
            init_search_nodes = (del_node, del_children[course^1])
            nodes = deque(init_search_nodes, maxlen=3)
            while nodes[-1] is not None:
                nodes.append(nodes[-1].children[course])
            replacement_node = nodes[1]

            nodes[0].children[course ^ (nodes[0] is del_node)] = \
                replacement_node.children[course^1] #detach replacement node
            replacement_node.children = del_node.children
        else:
            replacement_node = del_children[0] if len(del_children) == 1 else None
        del_node_parent.children[del_course] = replacement_node
        self.size -= 1

    def _clear_last_found(self):
        self.last_found = Node(object()) # a node that won't match anything

    def _try_find_node(self, k):
        if k == self.last_found.k:
            return self.last_found
        node = Cursor(self.anchor.children[1], stack=deque(maxlen=1)).find(k).node
        if node is not None:
            self.last_found = node
        return node

    def __contains__(self, k):
        return self._try_find_node(k) is not None

    def __len__(self):
        return self.size

    def _islice(self, start=None, stop=None, direction=None):
        if direction is None:
            direction = self.default_iter_direction
        valid_keys = KeyspaceSlice(None, stop, direction)
        cur = Cursor(self.anchor.children[1], direction)

        cur.find(limits[direction ^ 1] if (start is None) else start)
        if cur.has_result and cur.node.k in valid_keys:
            yield cur.node.k

        if stop is not None:
            yield from itertools.takewhile(valid_keys.contains_test, cur)
        else:
            yield from cur

    def __iter__(self):
        return self._islice()



class MapNode(Node):
    __slots__ = ('v')
    def __init__(self, k, v):
        super().__init__(k)
        self.v = v



class SearchTreeMap(SearchTree, collections.abc.MutableMapping):
    def __getitem__(self, k):
        if type(k) is slice:
            ksslice = KeyspaceSlice(None, None, self.default_iter_direction)[k]
            return SearchTreeMapSlice(self, ksslice)
        node = self._find(k).node
        if node is None:
            raise KeyError()
        return node

    def insert_or_replace(self, node):
        if not isinstance(node, MapNode):
            raise TypeError("expected MapNode")
        super().insert_or_replace(node)

    def __setitem__(self, k, v):
        node = MapNode(k, v)
        self.insert_or_replace(node)

    def __delitem__(self, k):
        try:
            self.delete(k)
        except LookupError:
            raise KeyError()



class SearchTreeMapSliceView(collections.abc.Mapping):
    def __init__(self, tree:SearchTreeMap, ksslice:KeyspaceSlice):
        self.tree = tree
        self.ksslice = ksslice

    def __contains__(self, k):
        return k in self.ksslice and k in tree

    def __getitem__(self, k):
        if type(k) is slice:
            return self.__class__(self.tree, self.ksslice[k])
        elif k not in self.ksslice:
            raise KeyError()
        return tree[k]

    def __iter__(self):
        return tree._islice(self.ksslice.start, self.ksslice.stop)

    def __len__(self):
        # what a horrible function
        return sum(1 for _ in iter(self))
