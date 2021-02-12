# copyright (c) 2021 Jason Forbes

import random, itertools, operator
from collections import deque, namedtuple



class Node:
    __slots__ = ('children','k')
    def __init__(self, k):
        self.children = [None, None]
        self.k = k

    def __repr__(self):
        cks = [getattr(c,'k',None) for c in self.children]
        return f"<{__name__}.{self.__class__.__name__} ({self.k}) {cks}>"



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



class SearchTree:
    class AnchorK:
        def __lt__(self, other):
            return True

    def __init__(self):
        self.size = 0
        self.anchor = Node(self.AnchorK())

    def insert(self, node_or_k):
        new_node = node_or_k if isinstance(node_or_k,Node) else Node(node_or_k)
        k = new_node.k
        drop_depth = random.getrandbits(self.size.bit_length()).bit_length() if self.size else 0
        node = self.anchor
        for depth in itertools.count():
            children = node.children
            course = int(node.k < k);
            node = children[course]

            if node is None or depth == drop_depth:
                children[course] = new_node

                if node is not None:
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
                break
        self.size += 1

    def delete(self, k):
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

    def __contains__(self, k):
        return bool(Cursor(self.anchor.children[1], stack=deque(maxlen=1)).find(k))

    def __len__(self):
        return self.size

    def islice(self, start=None, stop=None, reversed=False):
        direction = int(not reversed)
        sort_op = (operator.gt, operator.lt)[direction]
        continue_test = lambda k: sort_op(k, stop)
        cur = Cursor(self.anchor.children[1], direction)

        cur.find(limits[direction ^ 1] if (start is None) else start)
        if cur.has_result and (stop is None or continue_test(cur.node.k)):
            yield cur.node.k

        if stop is not None:
            yield from itertools.takewhile(continue_test, cur)
        else:
            yield from cur

    def __iter__(self):
        return self.islice()
