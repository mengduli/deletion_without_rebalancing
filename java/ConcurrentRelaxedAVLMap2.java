/*
 * This is the code for non-blocking ravl tree
 */

import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class ConcurrentRelaxedAVLMap2<K, V> implements ConcurrentTreeMap<K, V> {

	private final int d;
	private static final int DEFAULT_d = 6;
	private final Node root;
	private final Node missing;
	private final Operation dummy;
	private final Comparator<? super K> comparator;
	private final AtomicReferenceFieldUpdater<ConcurrentRelaxedAVLMap2.Node, ConcurrentRelaxedAVLMap2.Operation> updateOp;
	private final AtomicReferenceFieldUpdater<ConcurrentRelaxedAVLMap2.Node, ConcurrentRelaxedAVLMap2.Node> updateLeft,
			updateRight;

	public ConcurrentRelaxedAVLMap2() {
		this(DEFAULT_d, null);
	}

	public ConcurrentRelaxedAVLMap2(final Comparator<? super K> cmp) {
		this(DEFAULT_d, cmp);
	}

	public ConcurrentRelaxedAVLMap2(final int allowedViolationsPerPath) {
		this(allowedViolationsPerPath, null);
	}

	public ConcurrentRelaxedAVLMap2(final int allowedViolationsPerPath, final Comparator<? super K> cmp) {
		d = allowedViolationsPerPath;
		comparator = cmp;
		dummy = new Operation();

		/* sentinel node: key = NULL, value = NULL, rank = infinite */
		missing = new Node(-1, null, -1, null, null, dummy);
		root = new Node(null, null, Integer.MAX_VALUE,
				new Node(null, null, Integer.MAX_VALUE, new Node(missing), new Node(missing), dummy), new Node(missing),
				dummy);
		updateOp = AtomicReferenceFieldUpdater.newUpdater(Node.class, Operation.class, "op");
		updateLeft = AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "left");
		updateRight = AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "right");
		
		this.recordList = new ArrayList<Record<K>>();
	}

	// get tree size
	public final int size() {
		return sequentialSize(root);
	}

	// get value from dictionary
	public final boolean containsKey(final K key) {
		return get(key) != null;
	}

	public final V get(final K key) {
		final Comparable<? super K> k = comparable(key);
		Node l = root.left.left;
		if (l == null)
			return null; // no keys in data structure
		if (isMissing(l)) {
			return null;
		}
		while (!isLeaf(l)) {
			l = (k.compareTo((K) l.key) < 0) ? l.left : l.right;
		}
		return (k.compareTo((K) l.key) == 0) ? (V) l.value : null;
	}

	public final V put(final K key, final V value) {
		final Comparable<? super K> k = comparable(key);
		Operation op = null;
		Node p = null, l = null;
		int count = 0;
		Record record = new Record(INSERT_OPERATION, key);

		while (true) {
			while (op == null) {
				p = root;
				l = root.left;
				if (!isLeaf(l)) {
					count = 0;
					p = l;
					l = l.left; // note: before executing this line, l must have
								// key infinity, and l.left must not.
					while (!isLeaf(l)) {
						// compute the number of violation along the path
						if (d > 0 && (l.rank == p.rank))
							++count;
						p = l;
						l = (k.compareTo((K) l.key) < 0) ? l.left : l.right;
					}
				}

				// if we find the key in the tree already
				if (l.key != null && k.compareTo((K) l.key) == 0) {
					recordList.add(record);
					return null;
				} else {
					op = createInsertOp(p, l, key, value, k);
				}
			}
			if (helpSCX(op, 0)) {
				recordList.add(record);
				// clean up violations if necessary
				if (d == 0) {
					if (l.rank == 0) {
						fixToKey(k);
					}

				} else {
					if (l.rank == 0 ) ++count;
					if (count >= d) {
						fixToKey(k);
					}

				}
				// we may have found the key and replaced its value (and, if so,
				// the old value is stored in the old node)
				return (V) l.value;
			}
			op = null;
		}
	}

	public final V remove(final K key) {
		final Comparable<? super K> k = comparable(key);
		Node gp, p = null, l = null;
		Operation op = null;
		int count = 0;
		Record record = new Record(DELETE_OPERATION, key);

		while (true) {
			while (op == null) {
				gp = root;
				p = root;
				l = root.left;
				if (!isLeaf(l)) {
					count = 0;
					gp = p;
					p = l;
					l = l.left; // note: before executing this line, l must have
								// key infinity, and l.left must not.
					while (!isLeaf(l)) {
						if (d > 0 && (l.rank == p.rank))
							++count;
						gp = p;
						p = l;
						l = (k.compareTo((K) l.key) < 0) ? l.left : l.right;
					}
				}

				// the key was not in the tree at the linearization point, so no
				// value was removed
				if (l.key == null || k.compareTo((K) l.key) != 0) {
					recordList.add(record);
					return (V) l.value;
				} else {
					op = createDeleteOp(gp, p, l);
				}
			}
			if (helpSCX(op, 0)) {
				recordList.add(record);
				// clean up violations if necessary
				if (d > 0) {
					if (count >= d)
						fixToKey(k);
				}
				// we deleted a key, so we return the removed value (saved in
				// the old node)
				return null;
			}
			op = null;
		}
	}

	public final void fixToKey(final Comparable<? super K> k) {

		while (true) {
			Node gp, p, l = root.left, ls = root.right;
			if (isLeaf(l))
				return; // only sentinels in tree...
			gp = root;
			p = l;
			l = l.left; // note: before executing this line, l must have key
						// infinity, and l.left must not.
			ls = l.right;
			while (true) {
				if (isLeaf(l))
					return; // if no violation, then the search hit a leaf, so
							// we can stop
				gp = p;
				p = l;
				l = (k.compareTo((K) l.key) < 0) ? l.left : l.right;
				ls = (k.compareTo((K) l.key) < 0) ? l.right : l.left;

				if (p.rank == l.rank) {
					final Operation op = createBalancingOp(gp, p, l);
					if (op != null) {
						helpSCX(op, 0);
					}
					break;
				}
				if ((p.rank == l.rank + 1) && (p.rank == ls.rank)) {
					final Operation op = createBalancingOp(gp, p, ls);
					if (op != null) {
						helpSCX(op, 0);

					}
					break;
				}

			}

		}
	}

	@Override
	public String toString() {
		return printTree(root, 0);
	}

	/*************************************************************************
	 * Helper methods
	 *************************************************************************/
	@SuppressWarnings("unchecked")
	private Comparable<? super K> comparable(final Object key) {
		if (key == null) {
			throw new NullPointerException();
		}
		if (comparator == null) {
			return (Comparable<? super K>) key;
		}
		return new Comparable<K>() {
			@SuppressWarnings("unchecked")
			public int compareTo(final K rhs) {
				return comparator.compare((K) key, rhs);
			}
		};
	}

	private Operation createInsertOp(final Node p, final Node l, final K key, final V value, Comparable k) {
		final Operation[] ops = new Operation[] { null };
		final Node[] nodes = new Node[] { null, l };

		if (!weakLLX(p, 0, ops, nodes))
			return null;

		if (l != p.left && l != p.right)
			return null;
		
		if (weakLLX(l.left) == null || weakLLX(l.right) == null) {
			return null;
		}

		// Compute the weight for the new l node
		// maintain sentinel rank as infinite
		final int newRank = (isSentinel(l) ? Integer.MAX_VALUE : 0);

		// Build new sub-tree
		final Node newLeaf = new Node(key, value, 0, new Node(missing), new Node(missing), dummy);
		final Node newL = new Node(l.key, l.value, newRank, l.left, l.right, dummy);
		final Node newP;
		if (l.key == null || k.compareTo(l.key) < 0) {
			newP = new Node(l.key, l.value, l.rank, newLeaf, newL, dummy);
		} else {
			newP = new Node(key, value, l.rank, newL, newLeaf, dummy);
		}
		return new Operation(nodes, ops, newP);
	}

	private Operation createDeleteOp(final Node gp, final Node p, final Node l) {
		final Operation[] ops = new Operation[] { null, null, null };
		final Node[] nodes = new Node[] { null, null, null };

		if (!weakLLX(gp, 0, ops, nodes))
			return null;
		if (!weakLLX(p, 1, ops, nodes))
			return null;

		if (p != gp.left && p != gp.right)
			return null;
		final boolean left = (l == p.left);
		if (!left && l != p.right)
			return null;
		final Node s = left ? p.right : p.left;

		if (!weakLLX(l, 2, ops, nodes))
			return null;

		// Now, if the op. succeeds, all structure is guaranteed to be just as
		// we verified
		return new Operation(nodes, ops, s);
	}

	private Operation createBalancingOp(final Node pz, final Node z, final Node x) {

		final Operation oppz = weakLLX(pz);
		if (oppz == null)
			return null;
		final Node pzL = pz.left;
		final Node pzR = pz.right;
		final boolean pleft = (z == pzL);
		if (!pleft && z != pzR)
			return null;
		final Node zs = pleft ? pz.right : pz.left; // z's sibling

		final Operation opz = weakLLX(z);
		if (opz == null)
			return null;
		final Node zL = z.left;
		final Node zR = z.right;

		final Operation opzs = weakLLX(zs);
		if (opzs == null)
			return null;

		final boolean left = (x == zL);
		if (!left && x != zR)
			return null;
		final Node xs = left ? z.right : z.left; // x's sibling

		final Operation opx = weakLLX(x);
		if (opx == null)
			return null;

		final Operation opxs = weakLLX(xs);
		if (opxs == null)
			return null;

		// make y x's right child if x is a left child of z
		final Node y = left ? x.right : x.left;
		final Node ys = left ? x.left : x.right;

		final Operation opy = weakLLX(y);
		if (opy == null)
			return null;

		final Operation opys = weakLLX(ys);
		if (opys == null)
			return null;

		if (z.rank == x.rank) {

			if ((z.rank == xs.rank) || (z.rank == xs.rank + 1)) {

				// z is a 0,0-node or 0,1 node. promote z
				if (!canPromote(pz, z, zs)) {
					return null;
				}
				return createPromoteOp(pz, z, x, xs, oppz, opz, opx, opxs, left);
			} else {
				// z is a 0-i-node
				if ((x.rank >= y.rank + 2) || isMissing(y)) {
					// case 1 rotate on x
					return createRotate1Op(pz, z, x, xs, y, ys, oppz, opz, opx, opxs, opy, opys, left);
				} else if ((x.rank == y.rank + 1) && (x.rank == ys.rank + 1)) {
					if (!canPromote(pz, z, zs)) {
						return null;
					}
					// case 2 rotate on x
					return createRotate2Op(pz, z, x, xs, y, ys, oppz, opz, opx, opxs, opy, opys, left);
				} else {
					final Node yl = y.left;
					final Operation opyl = weakLLX(yl);
					if (opyl == null)
						return null;

					final Node yr = y.right;
					final Operation opyr = weakLLX(yr);
					if (opyr == null)
						return null;
					// double rotate on y
					return createDoubleRotateOp(pz, z, x, xs, y, ys, yl, yr, oppz, opz, opx, opxs, opy, opys, opyl,
							opyr, left);
				}
			}

		}

		return null;
	}

	private Operation createPromoteOp(final Node pz, final Node z, final Node x, final Node xs, final Operation oppz,
			final Operation opz, final Operation opx, final Operation opxs, final boolean left) {

		/*
		 * if x is z's left child, newZ.left = x, newZ.right = xs. otherwise,
		 * newZ.left = xs, newZ.right = x
		 */
		final Node newZ = left ? new Node(z.key, z.value, z.rank + 1, x, xs, dummy)
				: new Node(z.key, z.value, z.rank + 1, xs, x, dummy);
		return new Operation(new Node[] { pz, z }, new Operation[] { oppz, opz }, newZ);
	}

	private Operation createRotate1Op(final Node pz, final Node z, final Node x, final Node xs, final Node y,
			final Node ys, final Operation oppz, final Operation opz, final Operation opx, final Operation opxs,
			final Operation opy, final Operation opys, final boolean left) {

		/*
		 * if x is z's left child, newZ.left = y, newZ.right = xs, newX.left =
		 * ys, newX.right = newZ. otherwise, newZ.left = xs, newZ.right = y,
		 * newX.left = newZ, newX.right = ys
		 */
		final Node newZ = left ? new Node(z.key, z.value, z.rank - 1, y, xs, dummy)
				: new Node(z.key, z.value, z.rank - 1, xs, y, dummy);
		final Node newX = left ? new Node(x.key, x.value, x.rank, ys, newZ, dummy)
				: new Node(x.key, x.value, x.rank, newZ, ys, dummy);
		return new Operation(new Node[] { pz, z, x }, new Operation[] { oppz, opz, opx }, newX);

	}

	private Operation createRotate2Op(final Node pz, final Node z, final Node x, final Node xs, final Node y,
			final Node ys, final Operation oppz, final Operation opz, final Operation opx, final Operation opxs,
			final Operation opy, final Operation opys, final boolean left) {

		/*
		 * if x is z's left child, newZ.left = y, newZ.right = xs, newX.left =
		 * ys, newX.right = newZ. otherwise, newZ.left = xs, newZ.right = y,
		 * newX.left = newZ, newX.right = ys
		 */
		final Node newZ = left ? new Node(z.key, z.value, z.rank, y, xs, dummy)
				: new Node(z.key, z.value, z.rank, xs, y, dummy);
		final Node newX = left ? new Node(x.key, x.value, x.rank + 1, ys, newZ, dummy)
				: new Node(x.key, x.value, x.rank + 1, newZ, ys, dummy);
		return new Operation(new Node[] { pz, z, x }, new Operation[] { oppz, opz, opx }, newX);
	}

	private Operation createDoubleRotateOp(final Node pz, final Node z, final Node x, final Node xs, final Node y,
			final Node ys, final Node yl, final Node yr, final Operation oppz, final Operation opz, final Operation opx,
			final Operation opxs, final Operation opy, final Operation opys, final Operation opyl, final Operation opyr,
			final boolean left) {

		final Node newZ = left ? new Node(z.key, z.value, z.rank - 1, yr, xs, dummy)
				: new Node(z.key, z.value, z.rank - 1, xs, yl, dummy);
		final Node newX = left ? new Node(x.key, x.value, x.rank - 1, ys, yl, dummy)
				: new Node(x.key, x.value, x.rank - 1, yr, ys, dummy);
		final Node newY = left ? new Node(y.key, y.value, y.rank + 1, newX, newZ, dummy)
				: new Node(y.key, y.value, y.rank + 1, newZ, newX, dummy);
		return new Operation(new Node[] { pz, z, x, y }, new Operation[] { oppz, opz, opx, opy }, newY);

	}

	private boolean canPromote(final Node pz, final Node z, final Node zs) {
		return !((pz.rank == z.rank) || // cannot promote z otherwise created
										// -1-node
		((pz.rank == z.rank + 1) && (pz.rank == zs.rank))); // promote z
															// will make
															// pz a
															// 0,0-node
	}

	private boolean isSentinel(final Node node) {
		return (node.key == null || node == root.left);
	}

	private Operation weakLLX(final Node r) {
		final Operation rinfo = r.op;
		final int state = rinfo.state;
		if (state == Operation.STATE_ABORTED || (state == Operation.STATE_COMMITTED && !r.marked)) {
			return rinfo;
		}
		if (rinfo.state == Operation.STATE_INPROGRESS) {
			helpSCX(rinfo, 1);
		} else if (r.op.state == Operation.STATE_INPROGRESS) {
			helpSCX(r.op, 1);
		}
		return null;
	}

	// helper function to use the results of a weakLLX more conveniently
	private boolean weakLLX(final Node r, final int i, final Operation[] ops, final Node[] nodes) {
		if ((ops[i] = weakLLX(r)) == null)
			return false;
		nodes[i] = r;
		return true;
	}

	private boolean helpSCX(final Operation op, int i) {
		// get local references to some fields of op, in case we later null out
		// fields of op (to help the garbage collector)
		final Node[] nodes = op.nodes;
		final Operation[] ops = op.ops;
		final Node subtree = op.subtree;
		// if we see aborted or committed, no point in helping (already done).
		// further, if committed, variables may have been nulled out to help the
		// garbage collector.
		// so, we return.
		if (op.state != Operation.STATE_INPROGRESS)
			return true;

		// freeze sub-tree
		for (; i < ops.length; ++i) {
			// if work was not done
			if (!updateOp.compareAndSet(nodes[i], ops[i], op) && nodes[i].op != op) {
				if (op.allFrozen) {
					return true;
				} else {
					op.state = Operation.STATE_ABORTED;
					// help the garbage collector (must be AFTER we set state
					// committed or aborted)
					op.nodes = null;
					op.ops = null;
					op.subtree = null;
					return false;
				}
			}
		}
		op.allFrozen = true;
		for (i = 1; i < ops.length; ++i)
			nodes[i].marked = true; // finalize all but first node

		// CAS in the new sub-tree (child-cas)
		if (nodes[0].left == nodes[1]) {
			updateLeft.compareAndSet(nodes[0], nodes[1], subtree); // splice in
																	// new
																	// sub-tree
																	// (as a
																	// left
																	// child)
		} else { // assert: nodes[0].right == nodes[1]
			updateRight.compareAndSet(nodes[0], nodes[1], subtree); // splice in
																	// new
																	// sub-tree
																	// (as a
																	// right
																	// child)
		}
		op.state = Operation.STATE_COMMITTED;

		// help the garbage collector (must be AFTER we set state committed or
		// aborted)
		op.nodes = null;
		op.ops = null;
		op.subtree = null;
		return true;
	}

	private int sequentialSize(final Node node) {
		if (node == null || isMissing(node))
			return 0;
		if (isLeaf(node) && node.key != null)
			return 1;
		return sequentialSize(node.left) + sequentialSize(node.right);
	}

	private String printTree(Node node, int level) {
		if (node == null) {
			return "";
		}
		String s = "";
		for (int i = 0; i < level; ++i) {
			s += "\t";
		}

		if (!isSentinel(node)) {
			s += "(" + node.key + ", " + node.rank + ")\n";
		}

		if (!isMissing(node.left)) {
			s += printTree(node.left, isSentinel(node) ? level : (level + 1));

		}

		if (!isMissing(node.right)) {
			s += printTree(node.right, isSentinel(node) ? level : (level + 1));
		}
		return s;
	}

	private boolean isLeaf(final Node node) {
		return (node != null) && (node.left != null) && isMissing(node.left);
	}

	private boolean isMissing(final Node node) {
		return (node != null) && (node.rank == -1);
	}

	/*************************************************************************
	 * Helper classes
	 *************************************************************************/
	public static final class Node {
		public final int rank;
		public final Object value;
		public volatile boolean marked;
		public volatile Operation op;
		public final Object key;
		public volatile Node left, right;

		public Node(final Node node) {
			this.key = node.key;
			this.value = node.value;
			this.rank = node.rank;
			this.left = node.left;
			this.right = node.right;
			this.op = node.op;
		}

		public Node(final Object key, final Object value, final int rank, final Node left, final Node right,
				final Operation op) {
			this.key = key;
			this.value = value;
			this.rank = rank;
			this.left = left;
			this.right = right;
			this.op = op;
		}

		public final boolean hasChild(final Node node) {
			return node == left || node == right;
		}
	}

	public static final class Operation {
		final static int STATE_INPROGRESS = 0;
		final static int STATE_ABORTED = 1;
		final static int STATE_COMMITTED = 2;

		volatile Node subtree;
		volatile Node[] nodes;
		volatile Operation[] ops;
		volatile int state;
		volatile boolean allFrozen;

		public Operation() { // create an inactive operation (a no-op) [[ we do
								// this to avoid the overhead of inheritance ]]
			nodes = null;
			ops = null;
			subtree = null;
			this.state = STATE_ABORTED; // cheap trick to piggy-back on a
										// pre-existing check for active
										// operations
		}

		public Operation(final Node[] nodes, final Operation[] ops, final Node subtree) {
			this.nodes = nodes;
			this.ops = ops;
			this.subtree = subtree;
		}
	}
	
	/*
	 * The following are for test uses
	 */
	private ArrayList<Record<K>> recordList;
	static final int INSERT_OPERATION = 0;
	static final int DELETE_OPERATION = 1;
	
	public int height() {
		return treeHeightNode(root.left.left);
	}
	public static final class Record<K> {
		public final int operation;
		public final K key;

		public Record(final int operation, final K key) {
			this.operation = operation;
			this.key = key;
		}

		@Override
		public String toString() {
			return "Record [operation=" + operation + ", key=" + key + "]";
		}
	}
	
	private int treeHeightNode(final Node node) {
		if (node == null) {
			return 0;
		} else if (isMissing(node)) {
			return 1;
		} else {
			return 1 + Math.max(treeHeightNode(node.left), treeHeightNode(node.right));
		}
	}
	
	public ArrayList<K> storeKeyInKeyList() {
		ArrayList<K> keyList = new ArrayList<K>();
		for(Record record : recordList) {
			if (!keyList.contains((K)record.key) && record.operation == INSERT_OPERATION) {
					keyList.add((K)record.key);
			} else if (keyList.contains((K)record.key) && record.operation == DELETE_OPERATION) {
				keyList.remove((K)record.key);
			}
		}
		keyList.sort(comparator);
		System.out.println(keyList.toString());
		return keyList;
	}
	
	public ArrayList<K> collectTreeKey() {
		ArrayList<K> keyList = new ArrayList<K>();
		collectTreeKeyNode(root, keyList);
		keyList.sort(comparator);
		System.out.println(keyList.toString());
		return keyList;
	}
	
	public void collectTreeKeyNode(final Node node, ArrayList<K> keyList) {
		if (node == null) {
			return;
		} else if (isMissing(node)) {
			return;
		} else if (isLeaf(node) && node.key != null) {
			keyList.add((K)node.key);
		} else {
			collectTreeKeyNode(node.left, keyList);
			collectTreeKeyNode(node.right, keyList);
		}
	}
	
	public boolean foundViolation() {
		return foundViolationNode(root.left.left);
	}
	
	private boolean foundViolationNode (final Node node) {
		if (node == null) {
			return false;
		} else if (isMissing(node)) {
			return false;
		} else if (isLeaf(node)) {
			return false;
		} else if (node.rank == node.left.rank || node.rank == node.right.rank) {
			return true;
		} else {
			if (foundViolationNode(node.left)) {
				return true;
			} else if (foundViolationNode(node.right)){
				return true;
			}
		}
		return false;
	}
}
