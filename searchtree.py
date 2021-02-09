# copyright (c) 2021 Jason Forbes

import random, itertools, collections

class Node:
    __slots__ = ('children','k','v')
    def __init__(self, k, v=None):
        self.children = [None, None]
        self.k = k
        if v is not None:
            self.v = v
    
class SearchTree:
    class AnchorK:
        def __lt__(self, other):
            return True

    def __init__(self):
        self.size = 0
        self.anchor = Node(self.AnchorK())

    def lookup_node_and_parents(self, k, maxlen=None, anchor=False):
        """Return a deque of a node (specified by k) and its ancestors, with
        order starting closest to root and ending with Node(k). If k is not
        found, then the last element in the deque will be None, and the others
        will be the nodes tested before the search exhausted. `maxlen`
        describes the size of the deque returned, which can be None or an int
        greater than zero. To enable returning the tree's anchor node set
        `anchor` to True, but be warned that the tree's anchor node does not
        have a valid k value."""
        #             anchor:      root:
        init_nodes = [self.anchor, self.anchor.children[1]][int(not anchor):]
        nodes = collections.deque(init_nodes, maxlen)
        while True:
            n = nodes[-1]
            if n is None or n.k == k:
                return nodes
            nodes.append(n.children[int(n.k < k)])
        return nodes

    def insert(self, k, v=None):
        new_node = Node(k, v)
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
        raise NotImplementedError("still bugged, will fix later")
        del_node_parent, del_node = self.lookup_node_and_parents(k,2,anchor=True)
        if del_node is None:
            raise LookupError(k)
        del_course = del_node_parent.children.index(del_node)
        del_children = [n for n in del_node.children if n is not None]
        if len(del_children) == 2:
            #find a suitable replacement node
            course = random.getrandbits(1)
            init_search_nodes = (del_node, del_children[course^1])
            nodes = collections.deque(init_search_nodes, maxlen=3)
            while nodes[-1] is not None:
                nodes.append(nodes[-1].children[course])
            replacement_node = nodes[1]

            nodes[0].children[course] = replacement_node.children[course^1] #detach replacement node
            replacement_node.children = del_children
            if del_children[course^1] is replacement_node:
                replacement_node.children[course^1] = None
        else:
            replacement_node = del_children[0] if len(del_children) == 1 else None
        del_node_parent.children[del_course] = replacement_node
        self.size -= 1

    def __contains__(self, k):
        return self.lookup_node_and_parents(k, 1)[0] is not None

    def __len__(self):
        return self.size

    def __iter__(self):
        path_nodes = []
        path_course_taken = []
        node = self.anchor
        course = 1
        while path_nodes or course == 1:
            # try move
            if course & -2: #go up
                node = path_nodes.pop()
                course = path_course_taken.pop()
            elif node.children[course] is not None: #try go down
                path_nodes.append(node)
                path_course_taken.append(course)
                node = node.children[course]
                course = -1
            # pivot ccw
            course += 1
            if course == 1:
                yield node.k
