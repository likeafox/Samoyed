# copyright (c) 2021 Jason Forbes

import random, unittest
from searchtree import directions, limits, Node, Cursor, SearchTree

class BabyTreeBuilder(SearchTree):
    def __init__(self, input):
        self.input = input

    def build(self):
        def translate_input_to_rows():
            for l in self.input.splitlines():
                l = l.strip()
                if not l:
                    continue
                yield [(Node(int(x)) if x != '.' else None) for x in l.split()]

        rows = translate_input_to_rows()
        [root] = next(rows)
        stack = [[root]]
        for row in rows:
            good_row_sz = len(stack[-1]) << 1
            if len(row) != good_row_sz:
                raise ValueError(f"Got {len(row)} elements in row; expected {good_row_sz}.")
            rowpairs = [[row[i],row[i+1]] for i in range(0, len(row), 2)]
            assert len(stack[-1]) == len(rowpairs)
            for parent, pair in zip(stack[-1], rowpairs):
                if any(pair):
                    parent.children = pair
            stack.append(row)

        return root

    def make_SearchTree(self, *args, **kwargs):
        tree = SearchTree(*args, **kwargs)
        tree.root = self.build()
        tree.size = calc_tree_sz(tree.root)
        return tree

def calc_tree_sz(t):
    return (1 + sum(map(calc_tree_sz, t.children))) if t else 0
    
def tree_eq(n1:Node, n2:Node):
    if None in (n1, n2):
        return n1 is n2
    else:
        return n1.k == n2.k and \
               tree_eq(n1.children[0], n2.children[0]) and \
               tree_eq(n1.children[1], n2.children[1])

def predetermine_random_result(code, result, **kwargs):
    for i in range(1,10000):
        random.seed(i)
        locals_ = kwargs.copy()
        exec("test_result_ = " + code, globals(), locals_)
        if locals_['test_result_'] == result:
            random.seed(i)
            return
    else:
        raise RuntimeError("Gave up trying to predetermine random result.")



class TestBabyTreeBuilder(unittest.TestCase):
    def test_babytree_multiline(self):
        builder = BabyTreeBuilder("""
                 5
             3       7
           2   4   6   8
        """)
        root = builder.build()
        self.assertEqual(5, root.k)
        self.assertEqual(7, root.children[1].k)
        self.assertEqual(6, root.children[1].children[0].k)
        self.assertEqual(8, root.children[1].children[1].k)
        self.assertIs(None, root.children[1].children[1].children[0])
        self.assertIs(None, root.children[1].children[1].children[1])

    def test_babytree_blanks(self):
        builder = BabyTreeBuilder("""
                 5
             3       .
           2   .   .   .
        """)
        root = builder.build()
        self.assertEqual(5, root.k)
        self.assertEqual(3, root.children[0].k)
        self.assertIs(None, root.children[1])
        self.assertEqual(2, root.children[0].children[0].k)
        self.assertIs(None, root.children[0].children[1])
        self.assertIs(None, root.children[0].children[0].children[0])
        self.assertIs(None, root.children[0].children[0].children[1])

class TestTreeMetricUtilities(unittest.TestCase):
    def setUp(self):
        self.builder = BabyTreeBuilder("""
                 5
             3       7
           2   4   6   8
        """)

    def test_tree_eq(self):
        t1 = self.builder.build()
        t2 = self.builder.build()
        t3 = self.builder.build()
        t3.children[1].children[1] = None # delete k=8
        t4 = self.builder.build()
        t4.children[0].children[0].k = 1 # change k=2 to k=1
        self.assertTrue(tree_eq(t1, t2))
        self.assertFalse(tree_eq(t1, t3))
        self.assertFalse(tree_eq(t1, t4))

    def test_calc_tree_sz(self):
        t = self.builder.build()
        t.children[1].children[1] = None
        self.assertEqual(calc_tree_sz(t), 6)

class TestCursor(unittest.TestCase):
    def setUp(self):
        self.builder = BabyTreeBuilder("""
                              22
                  8                       35
            5           17          29          40
         3     7     11    20    .     33    39    42
        1  4  6  .  9  15 19 21 .  .  .  34 .  .  .  44
        """)
        self.all_ks = sorted(int(x) for x in self.builder.input.split() if x != '.')
        assert 10 < len(self.all_ks) < 100

    def test_cursor_find_ok(self):
        t = self.builder.build()
        for k in self.all_ks:
            cur = Cursor(t).find(k)
            self.assertTrue(cur)
            self.assertEqual(cur.node.k, k)

    def test_cursor_find_fail(self):
        t = self.builder.build()
        cur = Cursor(t).find(16)
        self.assertFalse(cur)
        self.assertEqual(cur.path_nodes[-1].k, 15)

    def test_cursor_find_limit(self):
        t = self.builder.build()
        cur = Cursor(t).find(limits[directions.left])
        self.assertEqual(cur.node.k, 1)

        cur = Cursor(t).find(limits[directions.right])
        self.assertEqual(cur.node.k, 44)

    def test_cursor_iter(self):
        t = self.builder.build()
        cur = Cursor(t).find(1)
        result = [cur.node.k] # result of current node isn't included in iterator
        result.extend(cur)
        self.assertEqual(result, self.all_ks)

class TestSearchTreeAdd(unittest.TestCase):
    def setUp(self):
        self.builder = BabyTreeBuilder("""
              8
          4       12
        2   6   10  .
        """)
        self.rand_code = "random.getrandbits(t.size.bit_length()).bit_length()"

    def test_replace(self):
        ref_tree = self.builder.build()
        for i in (8, 4, 10):
            t = self.builder.make_SearchTree()
            node = Node(i)
            t.insert_or_replace(node)
            self.assertIs(Cursor(t.root).find(i).node, node)
            self.assertTrue(tree_eq(ref_tree, t.root))

    def test_append(self):
        #empty append
        t = SearchTree()
        t.insert_or_replace(1)
        self.assertTrue(tree_eq(t.root, Node(1)))

        #non-emptys
        t = self.builder.make_SearchTree()
        predetermine_random_result(self.rand_code, 2, t=t)
        t.insert_or_replace(13)
        ref_tree = BabyTreeBuilder("""
              8
          4       12
        2   6   10  13
        """).build()
        self.assertTrue(tree_eq(t.root, ref_tree))

        t = self.builder.make_SearchTree()
        predetermine_random_result(self.rand_code, 3, t=t)
        t.insert_or_replace(1)
        ref_tree = BabyTreeBuilder("""
               8
           4       12
         2   6   10  .
        1 . . . . . . .
        """).build()
        self.assertTrue(tree_eq(t.root, ref_tree))

    def test_displace(self):
        t = self.builder.make_SearchTree()
        predetermine_random_result(self.rand_code, 1, t=t)
        t.insert_or_replace(5)
        ref_tree = BabyTreeBuilder("""
               8
           5       12
         4   6   10  .
        2 . . . . . . .
        """).build()
        self.assertTrue(tree_eq(t.root, ref_tree))

    def test_double_displace(self):
        t = self.builder.make_SearchTree()
        predetermine_random_result(self.rand_code, 0, t=t)
        t.insert_or_replace(5)
        ref_tree = BabyTreeBuilder("""
               5
           4       8
         2   .   6   12
        . . . . . . 10 .
        """).build()
        self.assertTrue(tree_eq(t.root, ref_tree))

class TestSearchTreeRemove(unittest.TestCase):
    def setUp(self):
        self.builder = BabyTreeBuilder("""
               8
           4       12
         2   6   10  .
        1 . 5 . . 11 . .
        """)
        self.rand_code = "random.getrandbits(1)"
        self.t = self.builder.make_SearchTree()

    def test_delete_with_only_child(self):
        predetermine_random_result(self.rand_code, 0, t=self.t)
        self.t.delete(12)
        ref_tree = BabyTreeBuilder("""
               8
           4       10
         2   6   .   11
        1 . 5 . . . . .
        """).build()
        self.assertTrue(tree_eq(self.t.root, ref_tree))

    def test_delete_left(self):
        predetermine_random_result(self.rand_code, 1, t=self.t)
        self.t.delete(8)
        ref_tree = BabyTreeBuilder("""
               6
           4       12
         2   5   10   .
        1 . . . . 11 . .
        """).build()
        self.assertTrue(tree_eq(self.t.root, ref_tree))

    def test_delete_right(self):
        predetermine_random_result(self.rand_code, 0, t=self.t)
        self.t.delete(8)
        ref_tree = BabyTreeBuilder("""
               10
           4       12
         2   6   11  .
        1 . 5 . . . . .
        """).build()
        self.assertTrue(tree_eq(self.t.root, ref_tree))

class TestSearchTreeBatchOperations(unittest.TestCase):
    def test_shotgun_approach(self):
        random.seed(939)
        def rand_k():
            return random.randrange(0,20000)
        t = SearchTree()
        comp_set = set()
        for op in 'iidididididididididi':
            if op == 'i':
                for i in range(1000):
                    k = rand_k()
                    t.insert_or_replace(i)
                    comp_set.add(i)
            if op == 'd':
                for i in range(400):
                    k = rand_k()
                    self.assertEqual(k in t, k in comp_set)
                    if k in comp_set:
                        comp_set.remove(k)
                        t.delete(k)
        self.assertEqual(list(t), sorted(comp_set))

if __name__ == '__main__':
    unittest.main(verbosity=2)
