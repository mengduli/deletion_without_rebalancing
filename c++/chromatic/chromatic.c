/*
 * chromatic.c
 *
 *  Created on: Nov 17, 2015
 *      Author: mengdu
 */
#include <assert.h>
#include <limits.h>
#include "atomic_ops.h"
#include "chromatic.h"

volatile operation_t* dummy = null;
node_t* root = null;
int d = 0; // number of violations

int init_node(node_t* node_ptr, const unsigned long key, const unsigned long weight, volatile node_t* left, volatile node_t* right,
		volatile operation_t* op) {

	node_ptr->key = key;
	node_ptr->weight = weight;
	node_ptr->left = left;
	node_ptr->right = right;
	node_ptr->marked = false;
	node_ptr->op = op;
	return SUCCESS;
}

bool is_sentinel(volatile node_t* node) {
	return node->key == ULLONG_MAX || node == root->left->left;
}

bool has_child(volatile node_t* p, volatile node_t* c) {
	return p->left == c || p->right == c;
}

void print_node(volatile node_t* node) {
	if (!node)
		return;
	printf("(key:%ld, weight:%ld)\n", node->key, node->weight);
}

int init_dummy_op(volatile operation_t* op_ptr) {

	clear_op(op_ptr);
	op_ptr->all_frozen = false;
	op_ptr->state = STATE_ABORTED;
	return SUCCESS;
}

int init_op(operation_t* op_ptr) {
	clear_op(op_ptr);
	op_ptr->all_frozen = false;
	op_ptr->state = STATE_INPROGRESS;
	return SUCCESS;
}

void clear_op(volatile operation_t* op_ptr) {
	op_ptr->subtree = 0;
	op_ptr->ops_size = 0;
}


volatile operation_t* weak_llx(volatile node_t* node) {
	volatile operation_t* node_info = node->op;
	const int state = node_info->state;
	if (state == STATE_ABORTED || (state == STATE_COMMITTED && !node->marked)) {
		return node_info;
	}
	if (node_info->state == STATE_INPROGRESS) {
		help_scx(node_info, 1);
	} else if (node->op->state == STATE_INPROGRESS) {
		help_scx(node->op, 1);
	}
	return null;
}

bool help_scx(volatile operation_t* op, const int start_index) {

	// if we see aborted or committed, no point in helping (already done).
	// further, if committed, variables may have been nulled out to help the
	// garbage collector.
	// so, we return.
	if (op->state != STATE_INPROGRESS)
		return true;

	// freeze sub-tree
	for (int i = start_index; i < op->ops_size; ++i) {
		// if work was not done
		if (!AO_compare_and_swap((AO_t*)(&(op->nodes[i]->op)), (AO_t)(op->ops[i]),
				(AO_t)(op)) && op->nodes[i]->op != op) {
			if (op->all_frozen) {
				return true;
			} else {
				op->state = STATE_ABORTED;
				// help the garbage collector (must be AFTER we set state
				// committed or aborted)
//				clear_op(op);
				return false;
			}
		}
	}
	op->all_frozen = true;
	for (int i = 1; i < op->ops_size; ++i)
		op->nodes[i]->marked = true; // finalize all but first node

	// CAS in the new sub-tree (child-cas)
	if (op->nodes[0]->left == op->nodes[1]) {
		AO_compare_and_swap((AO_t*)(&(op->nodes[0]->left)), (AO_t)(op->nodes[1]),
				(AO_t)(op->subtree));
	} else { // assert: op->nodes[0].right == op->nodes[1]
		AO_compare_and_swap((AO_t*)(&(op->nodes[0]->right)), (AO_t)(op->nodes[1]),
				(AO_t)(op->subtree));
	}
	op->state = STATE_COMMITTED;

	// help the garbage collector (must be AFTER we set state committed or
	// aborted)
//	clear_op(op);
	return true;
}

int init_tree(const int all_violation_per_path) {
	int rc = SUCCESS;
	dummy = (operation_t*) xmalloc(sizeof(operation_t));
	rc = init_dummy_op(dummy);

	d = all_violation_per_path;

	node_t* sentinel = (node_t*) xmalloc(sizeof(node_t));
	rc = init_node(sentinel, ULONG_MAX, 1, null, null, dummy);

	root = (node_t*) xmalloc(sizeof(node_t));
	rc = init_node(root, ULONG_MAX, 1, sentinel, null, dummy);

	if (rc)
		return rc;

	return SUCCESS;
}

int tree_size() {
	return sequential_size(root);
}

bool get(const unsigned long key) {
	volatile node_t* l = root->left->left;
	if (!l)
		return false; // no keys in data structure
	while (l->left) {
		l = key < l->key ? l->left : l->right;
	}
	if (l->key == key) {
		return true;
	} else {
		return false;
	}
}

bool insert(const unsigned long key) {
	volatile operation_t* op = null;
	volatile node_t* p = null;
	volatile node_t* l = null;
	int count = 0;
	while (true) {
		while (op == null) {
			p = root;
			l = root->left;
			if (l->left != null) {
				count = 0;
				p = l;
				l = l->left; // note: before executing this line, l must have key infinity, and l.left must not.
				while (l->left != null) {
					if (d > 0
							&& (l->weight > 1
									|| (l->weight == 0 && p->weight == 0)))
						++count;
					p = l;
					l = key < l->key ? l->left : l->right;
				}
			}

			// if we find the key in the tree already
			if (l->key == key) {
				return false;
			} else {
				op = create_insert_operation(p, l, key);
			}
		}
		if (help_scx(op, 0)) {
			// clean up violations if necessary
			if (d == 0) {
				if (p->weight == 0 && l->weight == 1)
					fix_to_key(key);
			} else {
				if (count >= d)
					fix_to_key(key);
			}
			return true;
		}
		op = null;
	}
}

bool delete(const unsigned long key) {
	volatile node_t* gp = null;
	volatile node_t* p = null;
	volatile node_t* l = null;
	volatile operation_t* op = null;
	int count = 0;
	while (true) {
		while (op == null) {
			gp = null;
			p = root;
			l = root->left;
			if (l->left != null) {
				count = 0;
				gp = p;
				p = l;
				l = l->left; // note: before executing this line, l must have key infinity, and l->left must not.
				while (l->left != null) {
					if (d > 0
							&& (l->weight > 1
									|| (l->weight == 0 && p->weight == 0)))
						++count;
					gp = p;
					p = l;
					l = key < l->key ? l->left : l->right;
				}
			}

			// the key was not in the tree at the linearization point, so no value was removed
			if (l->key != key) {
				return false;
			} else {
				op = create_remove_operation(gp, p, l);
			}
		}
		if (help_scx(op, 0)) {
			// clean up violations if necessary
			if (d == 0) {
				if (p->weight > 0 && l->weight > 0 && !is_sentinel(p))
					fix_to_key(key);
			} else {
				if (count >= d)
					fix_to_key(key);
			}
			// we deleted a key, so we return the removed value (saved in the old node)
			return true;
		}
		op = null;
	}
}

void print_tree() {
	print_tree_node(root, 0);
}

int sequential_size(volatile node_t* node) {
	if (!node)
		return 0;
	if (!node->left && node->key != ULONG_MAX)
		return 1;
	return sequential_size(node->left) + sequential_size(node->right);
}

void print_tree_node(volatile node_t* node, const int level) {
	if (!node)
		return;

	for (int i = 0; i < level; ++i)
		printf(" ");
	print_node(node);

	if (node->left)
		print_tree_node(node->left, level + 1);

	if (node->right)
		print_tree_node(node->right, level + 1);
}

void fix_to_key(const unsigned long key) {
	while (true) {
		volatile node_t* ggp;
		volatile node_t* gp;
		volatile node_t* p;
		volatile node_t* l = root->left;
		if (l->left == null)
			return; // only sentinels in tree...
		ggp = gp = root;
		p = l;
		l = l->left; // note: before executing this line, l must have key infinity, and l.left must not.
		while (l->left != null && l->weight <= 1
				&& (l->weight != 0 || p->weight != 0)) {
			ggp = gp;
			gp = p;
			p = l;
			l = key < l->key ? l->left : l->right;
		}
		if (l->weight == 1)
			return; // if no violation, then the search hit a leaf, so we can stop

		volatile operation_t* op = create_balancing_operation(ggp, gp, p, l);
		if (op != null) {
			help_scx(op, 0);
		}
	}
}

volatile operation_t* create_insert_operation(volatile node_t* p,
		volatile node_t* l, const unsigned long key) {

	operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
	init_op(new_op);
	new_op->ops_size = INSERT_OPS_SIZE;

	new_op->nodes[0] = p;
	new_op->ops[0] = weak_llx(p);
	if (!new_op->ops[0])
		return null;

	if (l != p->left && l != p->right)
		return null;

	new_op->nodes[1] = l;
	new_op->ops[1] = weak_llx(l);
	if (!new_op->ops[1])
		return null;

	// Compute the weight for the new parent node
	const int new_weight = (is_sentinel(l) ? 1 : l->weight - 1); // (maintain sentinel weights at 1)

	// Build new sub-tree
	node_t* new_leaf = (node_t*) xmalloc(sizeof(node_t));
	init_node(new_leaf, key, 1, null, null, dummy);
	node_t* new_l = (node_t*) xmalloc(sizeof(node_t));
	init_node(new_l, l->key, 1, null, null, dummy);

	node_t* new_p = (node_t*) xmalloc(sizeof(node_t));
	if (key < l->key) {
		init_node(new_p, l->key, new_weight, new_leaf, new_l, dummy);

	} else {
		init_node(new_p, key, new_weight, new_l, new_leaf, dummy);;
	}
	new_op->subtree = new_p;

	return new_op;
}

volatile operation_t* create_remove_operation(volatile node_t* gp,
		volatile node_t* p, volatile node_t* l) {
	operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
	init_op(new_op);
	new_op->ops_size = REMOVE_OPS_SIZE;

	new_op->nodes[0] = gp;
	new_op->ops[0] = weak_llx(gp);
	if (!new_op->ops[0])
		return null;

	if (p != gp->left && p != gp->right)
		return null;

	new_op->nodes[1] = p;
	new_op->ops[1] = weak_llx(p);
	if (!new_op->ops[1])
		return null;

	const bool left = l == p->left;
	if (!left && l != p->right)
		return null;

	volatile node_t* s = left ? p->right : p->left;
	new_op->nodes[2] = s;
	new_op->ops[2] = weak_llx(s);
	if (!new_op->ops[2])
		return null;

	// Compute weight for the new node (to replace to deleted leaf l and parent p)
	const int new_weight = (is_sentinel(p) ? 1 : p->weight + s->weight); // weights of parent + sibling of deleted leaf

	// Build new sub-tree
	node_t* new_p = (node_t*) xmalloc(sizeof(node_t));
	init_node(new_p, s->key, new_weight, s->left, s->right, dummy);
	new_op->subtree = new_p;

	return new_op;
}

volatile operation_t* create_balancing_operation(volatile node_t* f,
		volatile node_t* fX, volatile node_t* fXX, volatile node_t* fXXX) {
	volatile operation_t* opf = weak_llx(f);
	if (opf == null || !has_child(f, fX))
		return null;

	volatile operation_t* opfX = weak_llx(fX);
	if (opfX == null)
		return null;
	volatile node_t* fXL = fX->left;
	volatile node_t* fXR = fX->right;
	const bool fXXleft = (fXX == fXL);
	if (!fXXleft && fXX != fXR)
		return null;

	volatile operation_t* opfXX = weak_llx(fXX);
	if (opfXX == null)
		return null;
	volatile node_t* fXXL = fXX->left;
	volatile node_t* fXXR = fXX->right;
	const bool fXXXleft = (fXXX == fXXL);
	if (!fXXXleft && fXXX != fXXR)
		return null;

	// Overweight violation
	if (fXXX->weight > 1) {
		if (fXXXleft) {
			volatile operation_t* opfXXL = weak_llx(fXXL);
			if (opfXXL == null)
				return null;
			return create_overweight_left_op(f, fX, fXX, fXXL, opf, opfX, opfXX,
					opfXXL, fXL, fXR, fXXR, fXXleft);

		} else {
			volatile operation_t* opfXXR = weak_llx(fXXR);
			if (opfXXR == null)
				return null;
			return create_overweight_right_op(f, fX, fXX, fXXR, opf, opfX,
					opfXX, opfXXR, fXR, fXL, fXXL, !fXXleft);
		}
		// Red-red violation
	} else {
		if (fXXleft) {
			if (fXR->weight == 0) {
				volatile operation_t* opfXR = weak_llx(fXR);
				if (opfXR == null)
					return null;

				operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
				init_op(new_op);
				new_op->ops_size = BLK_OPS_SIZE;

				new_op->nodes[0] = f;
				new_op->nodes[1] = fX;
				new_op->nodes[2] = fXX;
				new_op->nodes[3] = fXR;

				new_op->ops[0] = opf;
				new_op->ops[1] = opfX;
				new_op->ops[2] = opfXX;
				new_op->ops[3] = opfXR;
				return createBlkOp(new_op);

			} else if (fXXXleft) {
				operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
				init_op(new_op);

				new_op->ops_size = RB1_OPS_SIZE;

				new_op->nodes[0] = f;
				new_op->nodes[1] = fX;
				new_op->nodes[2] = fXX;

				new_op->ops[0] = opf;
				new_op->ops[1] = opfX;
				new_op->ops[2] = opfXX;
				return createRb1Op(new_op);
			} else {
				volatile operation_t* opfXXR = weak_llx(fXXR);
				if (opfXXR == null)
					return null;

				operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
				init_op(new_op);
				new_op->ops_size = RB2_OPS_SIZE;

				new_op->nodes[0] = f;
				new_op->nodes[1] = fX;
				new_op->nodes[2] = fXX;
				new_op->nodes[3] = fXXR;

				new_op->ops[0] = opf;
				new_op->ops[1] = opfX;
				new_op->ops[2] = opfXX;
				new_op->ops[3] = opfXXR;
				return createRb2Op(new_op);

			}
		} else {
			if (fXL->weight == 0) {
				volatile operation_t* opfXL = weak_llx(fXL);
				if (opfXL == null)
					return null;
				operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
				init_op(new_op);
				new_op->ops_size = BLK_OPS_SIZE;

				new_op->nodes[0] = f;
				new_op->nodes[1] = fX;
				new_op->nodes[2] = fXL;
				new_op->nodes[3] = fXX;

				new_op->ops[0] = opf;
				new_op->ops[1] = opfX;
				new_op->ops[2] = opfXL;
				new_op->ops[3] = opfXX;
				return createBlkOp(new_op);

			} else if (!fXXXleft) {
				operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
				init_op(new_op);
				new_op->ops_size = RB1SYM_OPS_SIZE;

				new_op->nodes[0] = f;
				new_op->nodes[1] = fX;
				new_op->nodes[2] = fXX;

				new_op->ops[0] = opf;
				new_op->ops[1] = opfX;
				new_op->ops[2] = opfXX;
				return createRb1SymOp(new_op);

			} else {
				volatile operation_t* opfXXL = weak_llx(fXXL);
				if (opfXXL == null)
					return null;
				operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
				init_op(new_op);
				new_op->ops_size = RB2SYM_OPS_SIZE;

				new_op->nodes[0] = f;
				new_op->nodes[1] = fX;
				new_op->nodes[2] = fXX;
				new_op->nodes[3] = fXXL;

				new_op->ops[0] = opf;
				new_op->ops[1] = opfX;
				new_op->ops[2] = opfXX;
				new_op->ops[3] = opfXXL;
				return createRb2SymOp(new_op);

			}
		}
	}
	return null;
}

volatile operation_t* create_overweight_left_op(volatile node_t* f,
		volatile node_t* fX, volatile node_t* fXX, volatile node_t* fXXL,
		volatile operation_t* opf, volatile operation_t* opfX,
		volatile operation_t* opfXX, volatile operation_t* opfXXL,
		volatile node_t* fXL, volatile node_t* fXR, volatile node_t* fXXR,
		const bool fXXlef) {
	if (fXXR->weight == 0) {
		if (fXX->weight == 0) {
			if (fXXlef) {
				if (fXR->weight == 0) {
					volatile operation_t* opfXR = weak_llx(fXR);
					if (opfXR == null)
						return null;

					operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
					init_op(new_op);

					new_op->ops_size = BLK_OPS_SIZE;

					new_op->nodes[0] = f;
					new_op->nodes[1] = fX;
					new_op->nodes[2] = fXX;
					new_op->nodes[3] = fXR;

					new_op->ops[0] = opf;
					new_op->ops[1] = opfX;
					new_op->ops[2] = opfXX;
					new_op->ops[3] = opfXR;
					return createBlkOp(new_op);

				} else { // assert: fXR->weight > 0
					volatile operation_t* opfXXR = weak_llx(fXXR);
					if (opfXXR == null)
						return null;

					operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
					init_op(new_op);
					new_op->ops_size = RB2_OPS_SIZE;

					new_op->nodes[0] = f;
					new_op->nodes[1] = fX;
					new_op->nodes[2] = fXX;
					new_op->nodes[3] = fXXR;

					new_op->ops[0] = opf;
					new_op->ops[1] = opfX;
					new_op->ops[2] = opfXX;
					new_op->ops[3] = opfXXR;
					return createRb2Op(new_op);

				}
			} else { // assert: fXX == fXR
				if (fXL->weight == 0) {
					volatile operation_t* opfXL = weak_llx(fXL);
					if (opfXL == null)
						return null;

					operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
					init_op(new_op);
					new_op->ops_size = BLK_OPS_SIZE;

					new_op->nodes[0] = f;
					new_op->nodes[1] = fX;
					new_op->nodes[2] = fXL;
					new_op->nodes[3] = fXX;

					new_op->ops[0] = opf;
					new_op->ops[1] = opfX;
					new_op->ops[2] = opfXL;
					new_op->ops[3] = opfXX;
					return createBlkOp(new_op);

				} else {
					operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
					init_op(new_op);
					new_op->ops_size = RB1SYM_OPS_SIZE;

					new_op->nodes[0] = f;
					new_op->nodes[1] = fX;
					new_op->nodes[2] = fXX;

					new_op->ops[0] = opf;
					new_op->ops[1] = opfX;
					new_op->ops[2] = opfXX;
					return createRb1SymOp(new_op);

				}
			}
		} else { // assert: fXX->weight > 0
			volatile operation_t* opfXXR = weak_llx(fXXR);
			if (opfXXR == null)
				return null;

			volatile node_t* fXXRL = fXXR->left;
			volatile operation_t* opfXXRL = weak_llx(fXXRL);
			if (opfXXRL == null)
				return null;

			if (fXXRL->weight > 1) {

				operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
				init_op(new_op);
				new_op->ops_size = W1_OPS_SIZE;

				new_op->nodes[0] = fX;
				new_op->nodes[1] = fXX;
				new_op->nodes[2] = fXXL;
				new_op->nodes[3] = fXXR;
				new_op->nodes[4] = fXXRL;

				new_op->ops[0] = opfX;
				new_op->ops[1] = opfXX;
				new_op->ops[2] = opfXXL;
				new_op->ops[3] = opfXXR;
				new_op->ops[4] = opfXXRL;
				return createW1Op(new_op);

			} else if (fXXRL->weight == 0) {
				operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
				init_op(new_op);
				new_op->ops_size = RB2SYM_OPS_SIZE;

				new_op->nodes[0] = fX;
				new_op->nodes[1] = fXX;
				new_op->nodes[2] = fXXR;
				new_op->nodes[3] = fXXRL;

				new_op->ops[0] = opfX;
				new_op->ops[1] = opfXX;
				new_op->ops[2] = opfXXR;
				new_op->ops[3] = opfXXRL;
				return createRb2SymOp(new_op);

			} else { // assert: fXXRL->weight == 1
				volatile node_t* fXXRLR = fXXRL->right;
				if (fXXRLR == null)
					return null;
				if (fXXRLR->weight == 0) {
					volatile operation_t* opfXXRLR = weak_llx(fXXRLR);
					if (opfXXRLR == null)
						return null;
					operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
					init_op(new_op);

					new_op->ops_size = W4_OPS_SIZE;

					new_op->nodes[0] = fX;
					new_op->nodes[1] = fXX;
					new_op->nodes[2] = fXXL;
					new_op->nodes[3] = fXXR;
					new_op->nodes[4] = fXXRL;
					new_op->nodes[5] = fXXRLR;

					new_op->ops[0] = opfX;
					new_op->ops[1] = opfXX;
					new_op->ops[2] = opfXXL;
					new_op->ops[3] = opfXXR;
					new_op->ops[4] = opfXXRL;
					new_op->ops[5] = opfXXRLR;
					return createW4Op(new_op);
				} else { // assert: fXXRLR->weight > 0
					volatile node_t* fXXRLL = fXXRL->left;
					if (fXXRLL == null)
						return null;
					if (fXXRLL->weight == 0) {
						volatile operation_t* opfXXRLL = weak_llx(fXXRLL);
						if (opfXXRLL == null)
							return null;

						operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
						init_op(new_op);
						new_op->ops_size = W3_OPS_SIZE;

						new_op->nodes[0] = fX;
						new_op->nodes[1] = fXX;
						new_op->nodes[2] = fXXL;
						new_op->nodes[3] = fXXR;
						new_op->nodes[4] = fXXRL;
						new_op->nodes[5] = fXXRLL;

						new_op->ops[0] = opfX;
						new_op->ops[1] = opfXX;
						new_op->ops[2] = opfXXL;
						new_op->ops[3] = opfXXR;
						new_op->ops[4] = opfXXRL;
						new_op->ops[5] = opfXXRLL;
						return createW3Op(new_op);
					} else { // assert: fXXRLL->weight > 0
						operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
						init_op(new_op);
						new_op->ops_size = W2_OPS_SIZE;

						new_op->nodes[0] = fX;
						new_op->nodes[1] = fXX;
						new_op->nodes[2] = fXXL;
						new_op->nodes[3] = fXXR;
						new_op->nodes[4] = fXXRL;

						new_op->ops[0] = opfX;
						new_op->ops[1] = opfXX;
						new_op->ops[2] = opfXXL;
						new_op->ops[3] = opfXXR;
						new_op->ops[4] = opfXXRL;
						return createW2Op(new_op);
					}
				}
			}
		}
	} else if (fXXR->weight == 1) {
		volatile operation_t* opfXXR = weak_llx(fXXR);
		if (opfXXR == null)
			return null;

		volatile node_t* fXXRL = fXXR->left;
		if (fXXRL == null)
			return null;
		volatile node_t* fXXRR = fXXR->right; // note: if fXXRR is null, then fXXRL is null, since tree is always a full binary tree, and children of leaves don't change
		if (fXXRR->weight == 0) {
			volatile operation_t* opfXXRR = weak_llx(fXXRR);
			if (opfXXRR == null)
				return null;
			operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
			init_op(new_op);
			new_op->ops_size = W5_OPS_SIZE;

			new_op->nodes[0] = fX;
			new_op->nodes[1] = fXX;
			new_op->nodes[2] = fXXL;
			new_op->nodes[3] = fXXR;
			new_op->nodes[4] = fXXRR;

			new_op->ops[0] = opfX;
			new_op->ops[1] = opfXX;
			new_op->ops[2] = opfXXL;
			new_op->ops[3] = opfXXR;
			new_op->ops[4] = opfXXRR;
			return createW5Op(new_op);
		} else if (fXXRL->weight == 0) {
			volatile operation_t* opfXXRL = weak_llx(fXXRL);
			if (opfXXRL == null)
				return null;
			operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
			init_op(new_op);
			new_op->ops_size = W6_OPS_SIZE;

			new_op->nodes[0] = fX;
			new_op->nodes[1] = fXX;
			new_op->nodes[2] = fXXL;
			new_op->nodes[3] = fXXR;
			new_op->nodes[4] = fXXRL;

			new_op->ops[0] = opfX;
			new_op->ops[1] = opfXX;
			new_op->ops[2] = opfXXL;
			new_op->ops[3] = opfXXR;
			new_op->ops[4] = opfXXRL;
			return createW6Op(new_op);
		} else {
			operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
			init_op(new_op);
			new_op->ops_size = PUSHUP_OPS_SIZE;

			new_op->nodes[0] = fX;
			new_op->nodes[1] = fXX;
			new_op->nodes[2] = fXXL;
			new_op->nodes[3] = fXXR;

			new_op->ops[0] = opfX;
			new_op->ops[1] = opfXX;
			new_op->ops[2] = opfXXL;
			new_op->ops[3] = opfXXR;
			return createPushOp(new_op);
		}
	} else {
		volatile operation_t* opfXXR = weak_llx(fXXR);
		if (opfXXR == null)
			return null;
		operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
		init_op(new_op);

		new_op->ops_size = W7_OPS_SIZE;

		new_op->nodes[0] = fX;
		new_op->nodes[1] = fXX;
		new_op->nodes[2] = fXXL;
		new_op->nodes[3] = fXXR;

		new_op->ops[0] = opfX;
		new_op->ops[1] = opfXX;
		new_op->ops[2] = opfXXL;
		new_op->ops[3] = opfXXR;
		return createW7Op(new_op);
	}
	return null;
}

volatile operation_t* create_overweight_right_op(volatile node_t* f,
		volatile node_t* fX, volatile node_t* fXX, volatile node_t* fXXR,
		volatile operation_t* opf, volatile operation_t* opfX,
		volatile operation_t* opfXX, volatile operation_t* opfXXR,
		volatile node_t* fXR, volatile node_t* fXL, volatile node_t* fXXL,
		const bool fXXright) {
	if (fXXL->weight == 0) {
		if (fXX->weight == 0) {
			if (fXXright) {
				if (fXL->weight == 0) {
					volatile operation_t* opfXL = weak_llx(fXL);
					if (opfXL == null)
						return null;
					operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
					init_op(new_op);
					new_op->ops_size = BLK_OPS_SIZE;

					new_op->nodes[0] = f;
					new_op->nodes[1] = fX;
					new_op->nodes[2] = fXL;
					new_op->nodes[3] = fXX;

					new_op->ops[0] = opf;
					new_op->ops[1] = opfX;
					new_op->ops[2] = opfXL;
					new_op->ops[3] = opfXX;
					return createBlkOp(new_op);
				} else { // assert: fXL->weight > 0
					volatile operation_t* opfXXL = weak_llx(fXXL);
					if (opfXXL == null)
						return null;
					operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
					init_op(new_op);
					new_op->ops_size = RB2SYM_OPS_SIZE;

					new_op->nodes[0] = f;
					new_op->nodes[1] = fX;
					new_op->nodes[2] = fXX;
					new_op->nodes[3] = fXXL;

					new_op->ops[0] = opf;
					new_op->ops[1] = opfX;
					new_op->ops[2] = opfXX;
					new_op->ops[3] = opfXXL;
					return createRb2SymOp(new_op);
				}
			} else { // assert: fXX == fXL
				if (fXR->weight == 0) {
					volatile operation_t* opfXR = weak_llx(fXR);
					if (opfXR == null)
						return null;
					operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
					init_op(new_op);
					new_op->ops_size = BLK_OPS_SIZE;

					new_op->nodes[0] = f;
					new_op->nodes[1] = fX;
					new_op->nodes[2] = fXX;
					new_op->nodes[3] = fXR;

					new_op->ops[0] = opf;
					new_op->ops[1] = opfX;
					new_op->ops[2] = opfXX;
					new_op->ops[3] = opfXR;
					return createBlkOp(new_op);
				} else {
					operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
					init_op(new_op);
					new_op->ops_size = RB1_OPS_SIZE;

					new_op->nodes[0] = f;
					new_op->nodes[1] = fX;
					new_op->nodes[2] = fXX;

					new_op->ops[0] = opf;
					new_op->ops[1] = opfX;
					new_op->ops[2] = opfXX;
					return createRb1Op(new_op);
				}
			}
		} else { // assert: fXX->weight > 0
			volatile operation_t* opfXXL = weak_llx(fXXL);
			if (opfXXL == null)
				return null;

			volatile node_t* fXXLR = fXXL->right;
			volatile operation_t* opfXXLR = weak_llx(fXXLR);
			if (opfXXLR == null)
				return null;

			if (fXXLR->weight > 1) {
				operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
				init_op(new_op);
				new_op->ops_size = W1SYM_OPS_SIZE;

				new_op->nodes[0] = fX;
				new_op->nodes[1] = fXX;
				new_op->nodes[2] = fXXL;
				new_op->nodes[3] = fXXR;
				new_op->nodes[4] = fXXLR;

				new_op->ops[0] = opfX;
				new_op->ops[1] = opfXX;
				new_op->ops[2] = opfXXL;
				new_op->ops[3] = opfXXR;
				new_op->ops[4] = opfXXLR;
				return createW1SymOp(new_op);
			} else if (fXXLR->weight == 0) {
				operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
				init_op(new_op);
				new_op->ops_size = RB2_OPS_SIZE;

				new_op->nodes[0] = fX;
				new_op->nodes[1] = fXX;
				new_op->nodes[2] = fXXL;
				new_op->nodes[3] = fXXLR;

				new_op->ops[0] = opfX;
				new_op->ops[1] = opfXX;
				new_op->ops[2] = opfXXL;
				new_op->ops[3] = opfXXLR;
				return createRb2Op(new_op);
			} else { // assert: fXXLR->weight == 1
				volatile node_t* fXXLRL = fXXLR->left;
				if (fXXLRL == null)
					return null;
				if (fXXLRL->weight == 0) {
					volatile operation_t* opfXXLRL = weak_llx(fXXLRL);
					if (opfXXLRL == null)
						return null;
					operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
					init_op(new_op);

					new_op->ops_size = W4SYM_OPS_SIZE;

					new_op->nodes[0] = fX;
					new_op->nodes[1] = fXX;
					new_op->nodes[2] = fXXL;
					new_op->nodes[3] = fXXR;
					new_op->nodes[4] = fXXLR;
					new_op->nodes[5] = fXXLRL;

					new_op->ops[0] = opfX;
					new_op->ops[1] = opfXX;
					new_op->ops[2] = opfXXL;
					new_op->ops[3] = opfXXR;
					new_op->ops[4] = opfXXLR;
					new_op->ops[5] = opfXXLRL;
					return createW4SymOp(new_op);
				} else { // assert: fXXLRL->weight > 0
					volatile node_t* fXXLRR = fXXLR->right;
					if (fXXLRR == null)
						return null;
					if (fXXLRR->weight == 0) {
						volatile operation_t* opfXXLRR = weak_llx(fXXLRR);
						if (opfXXLRR == null)
							return null;

						operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
						init_op(new_op);
						new_op->ops_size = W3SYM_OPS_SIZE;

						new_op->nodes[0] = fX;
						new_op->nodes[1] = fXX;
						new_op->nodes[2] = fXXL;
						new_op->nodes[3] = fXXR;
						new_op->nodes[4] = fXXLR;
						new_op->nodes[5] = fXXLRR;

						new_op->ops[0] = opfX;
						new_op->ops[1] = opfXX;
						new_op->ops[2] = opfXXL;
						new_op->ops[3] = opfXXR;
						new_op->ops[4] = opfXXLR;
						new_op->ops[5] = opfXXLRR;
						return createW3SymOp(new_op);
					} else { // assert: fXXLRR->weight > 0
						operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
						init_op(new_op);
						new_op->ops_size = W2SYM_OPS_SIZE;

						new_op->nodes[0] = fX;
						new_op->nodes[1] = fXX;
						new_op->nodes[2] = fXXL;
						new_op->nodes[3] = fXXR;
						new_op->nodes[4] = fXXLR;

						new_op->ops[0] = opfX;
						new_op->ops[1] = opfXX;
						new_op->ops[2] = opfXXL;
						new_op->ops[3] = opfXXR;
						new_op->ops[4] = opfXXLR;
						return createW2SymOp(new_op);
					}
				}
			}
		}
	} else if (fXXL->weight == 1) {
		volatile operation_t* opfXXL = weak_llx(fXXL);
		if (opfXXL == null)
			return null;

		volatile node_t* fXXLR = fXXL->right;
		if (fXXLR == null)
			return null;
		volatile node_t* fXXLL = fXXL->left; // note: if fXXLL is null, then fXXLR is null, since tree is always a full binary tree, and children of leaves don't change
		if (fXXLL->weight == 0) {
			volatile operation_t* opfXXLL = weak_llx(fXXLL);
			if (opfXXLL == null)
				return null;
			operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
			init_op(new_op);
			new_op->ops_size = W5SYM_OPS_SIZE;

			new_op->nodes[0] = fX;
			new_op->nodes[1] = fXX;
			new_op->nodes[2] = fXXL;
			new_op->nodes[3] = fXXR;
			new_op->nodes[4] = fXXLL;

			new_op->ops[0] = opfX;
			new_op->ops[1] = opfXX;
			new_op->ops[2] = opfXXL;
			new_op->ops[3] = opfXXR;
			new_op->ops[4] = opfXXLL;
			return createW5SymOp(new_op);
		} else if (fXXLR->weight == 0) {
			volatile operation_t* opfXXLR = weak_llx(fXXLR);
			if (opfXXLR == null)
				return null;
			operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
			init_op(new_op);
			new_op->ops_size = W6SYM_OPS_SIZE;

			new_op->nodes[0] = fX;
			new_op->nodes[1] = fXX;
			new_op->nodes[2] = fXXL;
			new_op->nodes[3] = fXXR;
			new_op->nodes[4] = fXXLR;

			new_op->ops[0] = opfX;
			new_op->ops[1] = opfXX;
			new_op->ops[2] = opfXXL;
			new_op->ops[3] = opfXXR;
			new_op->ops[4] = opfXXLR;
			return createW6SymOp(new_op);
		} else {
			operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
			init_op(new_op);
			new_op->ops_size = PUSHUPSYM_OPS_SIZE;

			new_op->nodes[0] = fX;
			new_op->nodes[1] = fXX;
			new_op->nodes[2] = fXXL;
			new_op->nodes[3] = fXXR;

			new_op->ops[0] = opfX;
			new_op->ops[1] = opfXX;
			new_op->ops[2] = opfXXL;
			new_op->ops[3] = opfXXR;
			return createPushSymOp(new_op);
		}
	} else {
		volatile operation_t* opfXXL = weak_llx(fXXL);
		if (opfXXL == null)
			return null;
		operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
		init_op(new_op);
		new_op->ops_size = W7SYM_OPS_SIZE;

		new_op->nodes[0] = fX;
		new_op->nodes[1] = fXX;
		new_op->nodes[2] = fXXL;
		new_op->nodes[3] = fXXR;

		new_op->ops[0] = opfX;
		new_op->ops[1] = opfXX;
		new_op->ops[2] = opfXXL;
		new_op->ops[3] = opfXXR;
		return createW7SymOp(new_op);
	}
	return null;
}

volatile operation_t* createBlkOp(volatile operation_t* new_op) {

	node_t* nodeXL = (node_t*) xmalloc(sizeof(node_t));
	node_t* nodeXR = (node_t*) xmalloc(sizeof(node_t));
	node_t* nodeX = (node_t*) xmalloc(sizeof(node_t));

	if (init_node(nodeXL, new_op->nodes[2]->key,1, new_op->nodes[2]->left,
			new_op->nodes[2]->right, dummy))
		return null;

	if (init_node(nodeXR, new_op->nodes[3]->key, 1, new_op->nodes[3]->left,
			new_op->nodes[3]->right, dummy))
		return null;

	const int weight = (is_sentinel(new_op->nodes[1]) ? 1 : new_op->nodes[1]->weight - 1);

	init_node(nodeX, new_op->nodes[1]->key, weight, nodeXL, nodeXR, dummy);

	new_op->subtree = nodeX;

	return new_op;
}

volatile operation_t* createRb1Op(volatile operation_t* new_op) {
	node_t* nodeXR = (node_t*) xmalloc(sizeof(node_t));
	node_t* nodeX = (node_t*) xmalloc(sizeof(node_t));

	if (init_node(nodeXR, new_op->nodes[1]->key, 0, new_op->nodes[2]->right,
			new_op->nodes[1]->right, dummy))
		return null;

	const int weight = new_op->nodes[1]->weight;

	if (init_node(nodeX, new_op->nodes[2]->key, weight,
			new_op->nodes[2]->left, nodeXR, dummy))
		return null;

	new_op->subtree = nodeX;

	return new_op;

}

volatile operation_t* createRb2Op(volatile operation_t* new_op) {
	node_t* nodeXL = (node_t*) xmalloc(sizeof(node_t));
	node_t* nodeXR = (node_t*) xmalloc(sizeof(node_t));
	node_t* nodeX = (node_t*) xmalloc(sizeof(node_t));

	if (init_node(nodeXL, new_op->nodes[2]->key, 0, new_op->nodes[2]->left,
			new_op->nodes[3]->left, dummy))
		return null;

	if (init_node(nodeXR, new_op->nodes[1]->key, 0, new_op->nodes[3]->right,
			new_op->nodes[1]->right, dummy))
		return null;

	const int weight = new_op->nodes[1]->weight;

	if (init_node(nodeX, new_op->nodes[3]->key, weight, nodeXL,
			nodeXR, dummy))
		return null;

	new_op->subtree = nodeX;

	return new_op;
}

volatile operation_t* createRb1SymOp(volatile operation_t* new_op) {
	node_t* nodeXL = (node_t*) xmalloc(sizeof(node_t));
	node_t* nodeX = (node_t*) xmalloc(sizeof(node_t));

	if (init_node(nodeXL, new_op->nodes[1]->key, 0, new_op->nodes[1]->left,
			new_op->nodes[2]->left, dummy))
		return null;

	const int weight = new_op->nodes[1]->weight;

	if (init_node(nodeX, new_op->nodes[2]->key, weight, nodeXL,
			new_op->nodes[2]->right, dummy))
		return null;

	new_op->subtree = nodeX;

	return new_op;
}

volatile operation_t* createRb2SymOp(volatile operation_t* new_op) {
	node_t* nodeXL = (node_t*) xmalloc(sizeof(node_t));
	node_t* nodeXR = (node_t*) xmalloc(sizeof(node_t));
	node_t* nodeX = (node_t*) xmalloc(sizeof(node_t));

	if (init_node(nodeXL, new_op->nodes[1]->key, 0, new_op->nodes[1]->left,
			new_op->nodes[3]->left, dummy))
		return null;

	if (init_node(nodeXR, new_op->nodes[2]->key, 0, new_op->nodes[3]->right,
			new_op->nodes[2]->right, dummy))
		return null;

	const int weight = new_op->nodes[1]->weight;

	if (init_node(nodeX, new_op->nodes[3]->key, weight, nodeXL,
			nodeXR, dummy))
		return null;

	new_op->subtree = nodeX;

	return new_op;
}

volatile operation_t* createW1Op(volatile operation_t* new_op) {
	node_t* nodeXXLL = (node_t*) xmalloc(sizeof(node_t));
	node_t* nodeXXLR = (node_t*) xmalloc(sizeof(node_t));
	node_t* nodeXXL = (node_t*) xmalloc(sizeof(node_t));
	node_t* nodeXX = (node_t*) xmalloc(sizeof(node_t));

	if (init_node(nodeXXLL, new_op->nodes[2]->key,
			new_op->nodes[2]->weight - 1, new_op->nodes[2]->left, new_op->nodes[2]->right, dummy))
		return null;

	if (init_node(nodeXXLR, new_op->nodes[4]->key,
			new_op->nodes[4]->weight - 1, new_op->nodes[4]->left, new_op->nodes[4]->right, dummy))
		return null;

	if (init_node(nodeXXL, new_op->nodes[1]->key, 1, nodeXXLL,
			nodeXXLR, dummy))
		return null;

	const int weight = new_op->nodes[1]->weight;

	if (init_node(nodeXX, new_op->nodes[3]->key, weight, nodeXXL,
			new_op->nodes[3]->right, dummy))
		return null;

	new_op->subtree = nodeXX;

	return new_op;
}

volatile operation_t* createW2Op(volatile operation_t* new_op) {

	node_t* nodeXXLL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXLL, new_op->nodes[2]->key,
			new_op->nodes[2]->weight - 1, new_op->nodes[2]->left, new_op->nodes[2]->right, dummy))
		return null;

	node_t* nodeXXLR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXLR, new_op->nodes[4]->key, 0, new_op->nodes[4]->left,
			new_op->nodes[4]->right, dummy))
		return null;

	node_t* nodeXXL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXL, new_op->nodes[1]->key, 1, nodeXXLL,
			nodeXXLR, dummy))
		return null;

	const int weight = new_op->nodes[1]->weight;

	node_t* nodeXX = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXX, new_op->nodes[3]->key, weight, nodeXXL,
			new_op->nodes[3]->right, dummy))
		return null;

	new_op->subtree = nodeXX;

	return new_op;
}

volatile operation_t* createW3Op(volatile operation_t* new_op) {
	node_t* nodeXXLLL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXLLL, new_op->nodes[2]->key,
			new_op->nodes[2]->weight - 1, new_op->nodes[2]->left, new_op->nodes[2]->right, dummy))
		return null;

	node_t* nodeXXLL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXLL, new_op->nodes[1]->key, 1, nodeXXLLL,
			new_op->nodes[5]->left, dummy))
		return null;

	node_t* nodeXXLR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXLR, new_op->nodes[4]->key, 1, new_op->nodes[5]->right,
			new_op->nodes[4]->right, dummy))
		return null;

	node_t* nodeXXL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXL, new_op->nodes[5]->key, 0, nodeXXLL,
			nodeXXLR, dummy))
		return null;

	const int weight = new_op->nodes[1]->weight;

	node_t* nodeXX = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXX, new_op->nodes[3]->key, weight, nodeXXL,
			new_op->nodes[3]->right, dummy))
		return null;

	new_op->subtree = nodeXX;

	return new_op;
}

volatile operation_t* createW4Op(volatile operation_t* new_op) {
	node_t* nodeXXLL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXLL, new_op->nodes[2]->key,
			new_op->nodes[2]->weight - 1, new_op->nodes[2]->left, new_op->nodes[2]->right, dummy))
		return null;

	node_t* nodeXXL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXL, new_op->nodes[1]->key, 1, nodeXXLL,
			new_op->nodes[4]->left, dummy))
		return null;

	node_t* nodeXXRL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXRL, new_op->nodes[5]->key, 1, new_op->nodes[5]->left,
			new_op->nodes[5]->right, dummy))
		return null;

	node_t* nodeXXR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXR, new_op->nodes[3]->key, 0, nodeXXRL,
			new_op->nodes[3]->right, dummy))
		return null;

	const int weight = new_op->nodes[1]->weight;

	node_t* nodeXX = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXX, new_op->nodes[4]->key, weight, nodeXXL,
			nodeXXR, dummy))
		return null;

	new_op->subtree = nodeXX;
	return new_op;
}

volatile operation_t* createW5Op(volatile operation_t* new_op) {
	node_t* nodeXXLL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXLL, new_op->nodes[2]->key,
			new_op->nodes[2]->weight - 1, new_op->nodes[2]->left, new_op->nodes[2]->right, dummy))
		return null;

	node_t* nodeXXL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXL, new_op->nodes[1]->key, 1, nodeXXLL,
			new_op->nodes[3]->left, dummy))
		return null;

	node_t* nodeXXR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXR, new_op->nodes[4]->key, 1, new_op->nodes[4]->left,
			new_op->nodes[4]->right, dummy))
		return null;

	const int weight = new_op->nodes[1]->weight;

	node_t* nodeXX = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXX, new_op->nodes[3]->key, weight, nodeXXL,
			nodeXXR, dummy))
		return null;

	new_op->subtree = nodeXX;
	return new_op;
}

volatile operation_t* createW6Op(volatile operation_t* new_op) {
	node_t* nodeXXLL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXLL, new_op->nodes[2]->key,
			new_op->nodes[2]->weight - 1, new_op->nodes[2]->left, new_op->nodes[2]->right, dummy))
		return null;

	node_t* nodeXXL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXL, new_op->nodes[1]->key, 1, nodeXXLL,
			new_op->nodes[4]->left, dummy))
		return null;

	node_t* nodeXXR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXR, new_op->nodes[3]->key, 1, new_op->nodes[4]->right,
			new_op->nodes[3]->right, dummy))
		return null;

	const int weight = new_op->nodes[1]->weight;

	node_t* nodeXX = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXX, new_op->nodes[4]->key, weight, nodeXXL,
			nodeXXR, dummy))
		return null;

	new_op->subtree = nodeXX;

	return new_op;
}

volatile operation_t* createW7Op(volatile operation_t* new_op) {
	node_t* nodeXXL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXL, new_op->nodes[2]->key,
			new_op->nodes[2]->weight - 1, new_op->nodes[2]->left, new_op->nodes[2]->right, dummy))
		return null;

	node_t* nodeXXR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXR, new_op->nodes[3]->key,
			new_op->nodes[3]->weight - 1, new_op->nodes[3]->left, new_op->nodes[3]->right, dummy))
		return null;

	const int weight = is_sentinel(new_op->nodes[1]) ? 1 : new_op->nodes[1]->weight + 1;

	node_t* nodeXX = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXX, new_op->nodes[1]->key, weight, nodeXXL,
			nodeXXR, dummy))
		return null;

	new_op->subtree = nodeXX;

	return new_op;
}

volatile operation_t* createW1SymOp(volatile operation_t* new_op) {
	node_t* nodeXXRL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXRL, new_op->nodes[4]->key,
			new_op->nodes[4]->weight - 1, new_op->nodes[4]->left, new_op->nodes[4]->right, dummy))
		return null;

	node_t* nodeXXRR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXRR, new_op->nodes[3]->key,
			new_op->nodes[3]->weight - 1, new_op->nodes[3]->left, new_op->nodes[3]->right, dummy))
		return null;

	node_t* nodeXXR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXR, new_op->nodes[1]->key, 1, nodeXXRL,
			nodeXXRR, dummy))
		return null;

	const int weight = new_op->nodes[1]->weight;

	node_t* nodeXX = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXX, new_op->nodes[2]->key, weight,
			new_op->nodes[2]->left, nodeXXR, dummy))
		return null;

	new_op->subtree = nodeXX;
	return new_op;
}

volatile operation_t* createW2SymOp(volatile operation_t* new_op) {
	node_t* nodeXXRL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXRL, new_op->nodes[4]->key, 0, new_op->nodes[4]->left,
			new_op->nodes[4]->right, dummy))
		return null;

	node_t* nodeXXRR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXRR, new_op->nodes[3]->key,
			new_op->nodes[3]->weight - 1, new_op->nodes[3]->left, new_op->nodes[3]->right, dummy))
		return null;

	node_t* nodeXXR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXR, new_op->nodes[1]->key, 1, nodeXXRL,
			nodeXXRR, dummy))
		return null;

	const int weight = new_op->nodes[1]->weight;

	node_t* nodeXX = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXX, new_op->nodes[2]->key, weight,
			new_op->nodes[2]->left, nodeXXR, dummy))
		return null;

	new_op->subtree = nodeXX;
	return new_op;
}

volatile operation_t* createW3SymOp(volatile operation_t* new_op) {
	node_t* nodeXXRL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXRL, new_op->nodes[4]->key, 1, new_op->nodes[4]->left,
			new_op->nodes[5]->left, dummy))
		return null;

	node_t* nodeXXRRR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXRRR, new_op->nodes[3]->key,
			new_op->nodes[3]->weight - 1, new_op->nodes[3]->left, new_op->nodes[3]->right, dummy))
		return null;

	node_t* nodeXXRR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXRR, new_op->nodes[1]->key, 1, new_op->nodes[5]->right,
			nodeXXRRR, dummy))
		return null;

	node_t* nodeXXR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXR, new_op->nodes[5]->key, 0, nodeXXRL,
			nodeXXRR, dummy))
		return null;

	const int weight = new_op->nodes[1]->weight;

	node_t* nodeXX = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXX, new_op->nodes[2]->key, weight,
			new_op->nodes[2]->left, nodeXXR, dummy))
		return null;

	new_op->subtree = nodeXX;
	return new_op;
}

volatile operation_t* createW4SymOp(volatile operation_t* new_op) {
	node_t* nodeXXLR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXLR, new_op->nodes[5]->key, 1, new_op->nodes[5]->left,
			new_op->nodes[5]->right, dummy))
		return null;

	node_t* nodeXXL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXL, new_op->nodes[2]->key, 0, new_op->nodes[2]->left,
			nodeXXLR, dummy))
		return null;

	node_t* nodeXXRR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXRR, new_op->nodes[3]->key,
			new_op->nodes[3]->weight - 1, new_op->nodes[3]->left, new_op->nodes[3]->right, dummy))
		return null;

	node_t* nodeXXR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXR, new_op->nodes[1]->key, 1, new_op->nodes[4]->right,
			nodeXXRR, dummy))
		return null;

	const int weight = new_op->nodes[1]->weight;

	node_t* nodeXX = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXX, new_op->nodes[4]->key, weight, nodeXXL,
			nodeXXR, dummy))
		return null;

	new_op->subtree = nodeXX;
	return new_op;
}

volatile operation_t* createW5SymOp(volatile operation_t* new_op) {
	node_t* nodeXXL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXL, new_op->nodes[4]->key, 1, new_op->nodes[4]->left,
			new_op->nodes[4]->right, dummy))
		return null;

	node_t* nodeXXRR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXRR, new_op->nodes[3]->key,
			new_op->nodes[3]->weight - 1, new_op->nodes[3]->left, new_op->nodes[3]->right, dummy))
		return null;

	node_t* nodeXXR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXR, new_op->nodes[1]->key, 1, new_op->nodes[2]->right,
			nodeXXRR, dummy))
		return null;

	const int weight = new_op->nodes[1]->weight;

	node_t* nodeXX = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXX, new_op->nodes[2]->key, weight, nodeXXL,
			nodeXXR, dummy))
		return null;

	new_op->subtree = nodeXX;

	return new_op;
}

volatile operation_t* createW6SymOp(volatile operation_t* new_op) {
	node_t* nodeXXL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXL, new_op->nodes[2]->key, 1, new_op->nodes[2]->left,
			new_op->nodes[4]->left, dummy))
		return null;

	node_t* nodeXXRR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXRR, new_op->nodes[3]->key,
			new_op->nodes[3]->weight - 1, new_op->nodes[3]->left, new_op->nodes[3]->right, dummy))
		return null;

	node_t* nodeXXR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXR, new_op->nodes[1]->key, 1, new_op->nodes[4]->right,
			nodeXXRR, dummy))
		return null;

	const int weight = new_op->nodes[1]->weight;

	node_t* nodeXX = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXX, new_op->nodes[4]->key, weight, nodeXXL,
			nodeXXR, dummy))
		return null;

	new_op->subtree = nodeXX;

	return new_op;
}

volatile operation_t* createW7SymOp(volatile operation_t* new_op) {
	node_t* nodeXXL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXL, new_op->nodes[2]->key,
			new_op->nodes[2]->weight - 1, new_op->nodes[2]->left, new_op->nodes[2]->right, dummy))
		return null;

	node_t* nodeXXR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXR, new_op->nodes[3]->key,
			new_op->nodes[3]->weight - 1, new_op->nodes[3]->left, new_op->nodes[3]->right, dummy))
		return null;

	const int weight = is_sentinel(new_op->nodes[1]) ? 1 : new_op->nodes[1]->weight + 1;

	node_t* nodeXX = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXX, new_op->nodes[1]->key, weight, nodeXXL,
			nodeXXR, dummy))
		return null;

	new_op->subtree = nodeXX;

	return new_op;
}

volatile operation_t* createPushOp(volatile operation_t* new_op) {
	node_t* nodeXXL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXL, new_op->nodes[2]->key,
			new_op->nodes[2]->weight - 1, new_op->nodes[2]->left, new_op->nodes[2]->right, dummy))
		return null;

	node_t* nodeXXR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXR, new_op->nodes[3]->key, 0, new_op->nodes[3]->left,
			new_op->nodes[3]->right, dummy))
		return null;

	const int weight = is_sentinel(new_op->nodes[1]) ? 1 : new_op->nodes[1]->weight + 1;

	node_t* nodeXX = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXX, new_op->nodes[1]->key, weight, nodeXXL,
			nodeXXR, dummy))
		return null;

	new_op->subtree = nodeXX;
	return new_op;
}

volatile operation_t* createPushSymOp(volatile operation_t* new_op) {
	node_t* nodeXXL = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXL, new_op->nodes[2]->key, 0, new_op->nodes[2]->left,
			new_op->nodes[2]->right, dummy))
		return null;

	node_t* nodeXXR = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXXR, new_op->nodes[3]->key,
			new_op->nodes[3]->weight - 1, new_op->nodes[3]->left, new_op->nodes[3]->right, dummy))
		return null;

	const int weight = is_sentinel(new_op->nodes[1]) ? 1 : new_op->nodes[1]->weight + 1;

	node_t* nodeXX = (node_t*) xmalloc(sizeof(node_t));
	if (init_node(nodeXX, new_op->nodes[1]->key, weight, nodeXXL,
			nodeXXR, dummy))
		return null;

	new_op->subtree = nodeXX;

	return new_op;
}


int height() {
	return height_node(root->left->left);
}


int height_node(volatile node_t* node) {
	if (!node) {
		return 0;
	} else if (!node->left) {
		return 1;
	} else {
		int left_height = height_node(node->left);
		int right_height = height_node(node->right);
		return left_height > right_height ? left_height + 1 : right_height + 1;
	}
}
