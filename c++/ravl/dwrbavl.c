/*
 * dwrbavl.c
 *
 *  Created on: Nov 15, 2015
 *      Author: mengdu
 */

#include <limits.h>
#include <assert.h>
#include "dwrbavl.h"
#include "atomic_ops.h"

volatile operation_t* dummy = null;
node_t* root = null;
int d = 0; // number of violations

int init_node(node_t* node_ptr, const unsigned long key, const unsigned long rank,
		volatile node_t* left, volatile node_t* right,
		volatile operation_t* op) {

	node_ptr->key = key;
	node_ptr->rank = rank;
	node_ptr->left = left;
	node_ptr->right = right;
	node_ptr->marked = false;
	node_ptr->op = op;
	return SUCCESS;
}

bool is_sentinel(volatile node_t* node) {
	return node && node->key == ULONG_MAX;
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

int init_tree(const int all_violation_per_path) {
	dummy = (operation_t*) xmalloc(sizeof(operation_t));
	init_dummy_op(dummy);

	node_t* sentinel = (node_t*) xmalloc(sizeof(node_t));
	init_node(sentinel, ULONG_MAX, ULONG_MAX, null, null, dummy);

	root = (node_t*) xmalloc(sizeof(node_t));
	init_node(root, ULONG_MAX, ULONG_MAX, sentinel, null, dummy);

	d = all_violation_per_path;

	return SUCCESS;
}

int tree_size() {
	return sequential_size(root);
}

int sequential_size(volatile node_t* node) {
	if (!node)
		return 0;
	if (!node->left && node->key != ULONG_MAX)
		return 1;
	return sequential_size(node->left) + sequential_size(node->right);
}

bool get(const unsigned long key) {
	volatile node_t* l = root->left->left;
	if (!l) {
		return false; // the key is not in the dictionary
	}
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
	volatile operation_t* op = 0;
	volatile node_t* p = 0;
	volatile node_t* l = 0;
	int count = 0;
	while (true) {
		while (!op) {
			p = root;
			l = root->left;
			if (l->left) {
				p = l;
				l = l->left;
				while (l->left) {
					if (d > 0 && (l->rank == p->rank))
						++count;
					p = l;
					l = key < l->key ? l->left : l->right;
				}
			}
			if (l->key == key) {
				return false;
			} else {
				op = create_insert_operation(p, l, key);
			}
		}
		if (help_scx(op, 0)) {
			if (d == 0) {
				if (l->rank == 0)
					fix_to_key(key);
			} else {
				if (count >= d)
					fix_to_key(key);
			}

			return true;
		}
		op = 0;
	}
}

bool delete(const unsigned long key) {
	volatile node_t* gp = 0;
	volatile node_t* p = 0;
	volatile node_t* l = 0;
	volatile operation_t* op = 0;
	while (true) {
		while (!op) {
			gp = root;
			p = root;
			l = root->left;
			if (l->left) {
				gp = p;
				p = l;
				l = l->left;
				while (l->left) {
					gp = p;
					p = l;
					l = key < l->key ? l->left : l->right;
				}
			}

			if (l->key != key) {
				return false; // the key is not in the dictionary
			} else {
				op = create_remove_operation(gp, p, l);
			}
		}
		if (help_scx(op, 0)) {
			return true;
		}
		op = 0;
	}
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


void fix_to_key(const unsigned long key) {
	while (true) {
		volatile node_t* gp;
		volatile node_t* p = root;
		volatile node_t* l = root->left;
		volatile node_t* ls = root->right;
		if (l->left == null)
			return; // only sentinels in tree...
		gp = p;
		p = l;
		l = l->left;
		ls = l->right;
		while (true) {
			if (!l->left)
				return; // if no violation, then the search hit a leaf, so we can stop
			gp = p;
			p = l;
			l = key < l->key ? l->left : l->right;
			ls = key < l->key ? l->right : l->left;
			volatile operation_t* op = null;
			if (l->rank == p->rank) {
				op = create_balancing_operation(gp, p, l);
				if (op != null) {
					help_scx(op, 0);
				}
				break;
			} else if (ls && l->rank == p->rank - 1 && ls->rank == p->rank) {
				op = create_balancing_operation(gp, p, ls);
				if (op != null) {
					help_scx(op, 0);
				}
				break;
			}


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

	const unsigned long new_rank = is_sentinel(l) ? ULONG_MAX : 0;

	node_t* new_leaf = (node_t*) xmalloc(sizeof(node_t));
	init_node(new_leaf, key, 0, null, null, dummy);

	node_t* new_l = (node_t*) xmalloc(sizeof(node_t));
	init_node(new_l, l->key, new_rank, l->left, l->right, dummy);

	node_t* new_p = (node_t*) xmalloc(sizeof(node_t));
	if (key < l->key) {
		init_node(new_p, l->key, l->rank, new_leaf, new_l, dummy);

	} else {
		init_node(new_p, key, l->rank, new_l, new_leaf, dummy);
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

	new_op->nodes[2] = l;
	new_op->ops[2] = weak_llx(l);
	if (!new_op->ops[2])
		return null;

	new_op->subtree = left ? p->right : p->left;
	return new_op;
}

volatile operation_t* create_balancing_operation(volatile node_t* pz,
		volatile node_t* z, volatile node_t* x) {
	volatile operation_t* oppz = weak_llx(pz);
	if (!oppz) {
		return 0;
	}
	const bool p_left = (z == pz->left);
	if (!p_left && z != pz->right)
		return 0;
	volatile node_t* zs = p_left ? pz->right : pz->left;

	volatile operation_t* opz = weak_llx(z);
	if (!opz) {
		return 0;
	}

	const bool left = (x == z->left);
	if (!left && x != z->right)
		return 0;
	volatile node_t* xs = left ? z->right : z->left;

	if (z->rank == x->rank) {
		if ((z->rank == xs->rank) || (z->rank == xs->rank + 1)) {
			// z is a 0,0-node or 0,1 node. promote z
			if (!can_promote(pz, z, zs)) {
				return 0;
			}
			return create_promote_op(pz, z, oppz, opz, left);
		} else {
			volatile operation_t* opx = weak_llx(x);
			if (!opx) {
				return 0;
			}

			// make y x's right child if x is a left child of z
			volatile node_t* y = left ? x->right : x->left;
			volatile node_t* ys = left ? x->left : x->right;
			// z is a 0-i-node
			if ((x->rank >= y->rank + 2) || !y) {
				// case 1 rotate on x
				return create_rotate1_op(pz, z, x, oppz, opz, opx, left);
			} else if ((x->rank == y->rank + 1) && (x->rank == ys->rank + 1)) {
				if (!can_promote(pz, z, zs)) {
					return 0;
				}
				// case 2 rotate on x
				return create_rotate2_op(pz, z, x, oppz, opz, opx, left);
			} else {
				volatile operation_t* opy = weak_llx(y);
				if (!opy) {
					return 0;
				}

				// double rotate on y
				return create_double_rotate_op(pz, z, x, y, oppz, opz, opx, opy,
						left);
			}
		}
	}

	return null;
}

volatile operation_t* create_promote_op(volatile node_t* pz, volatile node_t* z,
		volatile operation_t* oppz, volatile operation_t* opz, const bool left) {
	operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
	init_op(new_op);
	new_op->ops_size = PROMOTE_OPS_SIZE;

	new_op->nodes[0] = pz;
	new_op->nodes[1] = z;

	new_op->ops[0] = oppz;
	new_op->ops[1] = opz;


	node_t* new_z = (node_t*) xmalloc(sizeof(node_t));
	init_node(new_z, z->key, z->rank + 1, z->left, z->right, dummy);
	new_op->subtree = new_z;

	return new_op;
}

volatile operation_t* create_rotate1_op(volatile node_t* pz, volatile node_t* z,
		volatile node_t* x, volatile operation_t* oppz,
		volatile operation_t* opz, volatile operation_t* opx, const bool left) {
	operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
	init_op(new_op);
	new_op->ops_size = ROTATE_OPS_SIZE;

	new_op->nodes[0] = pz;
	new_op->nodes[1] = z;
	new_op->nodes[2] = x;

	new_op->ops[0] = oppz;
	new_op->ops[1] = opz;
	new_op->ops[2] = opx;

	node_t* new_z = (node_t*) xmalloc(sizeof(node_t));
	node_t* new_x = (node_t*) xmalloc(sizeof(node_t));
	if (left) {
		init_node(new_z, z->key, z->rank - 1, x->right, z->right, dummy);
		init_node(new_x, x->key, x->rank, x->left, new_z, dummy);
	} else {
		init_node(new_z, z->key, z->rank - 1, z->left, x->left, dummy);
		init_node(new_x, x->key, x->rank, new_z, x->right, dummy);
	}
	new_op->subtree = new_x;

	return new_op;
}

volatile operation_t* create_rotate2_op(volatile node_t* pz, volatile node_t* z,
		volatile node_t* x, volatile operation_t* oppz,
		volatile operation_t* opz, volatile operation_t* opx, const bool left) {
	operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
	init_op(new_op);
	new_op->ops_size = ROTATE_OPS_SIZE;

	new_op->nodes[0] = pz;
	new_op->nodes[1] = z;
	new_op->nodes[2] = x;

	new_op->ops[0] = oppz;
	new_op->ops[1] = opz;
	new_op->ops[2] = opx;

	node_t* new_z = (node_t*) xmalloc(sizeof(node_t));
	node_t* new_x = (node_t*) xmalloc(sizeof(node_t));
	if (left) {
		init_node(new_z, z->key, z->rank, x->right, z->right, dummy);
		init_node(new_x, x->key, x->rank + 1, x->left, new_z, dummy);
	} else {
		init_node(new_z, z->key, z->rank, z->left, x->left, dummy);
		init_node(new_x, x->key, x->rank + 1, new_z, x->right, dummy);
	}
	new_op->subtree = new_x;

	return new_op;
}

volatile operation_t* create_double_rotate_op(volatile node_t* pz,
		volatile node_t* z, volatile node_t* x, volatile node_t* y,
		volatile operation_t* oppz, volatile operation_t* opz,
		volatile operation_t* opx, volatile operation_t* opy, const bool left) {

	operation_t* new_op = (operation_t*) xmalloc(sizeof(operation_t));
	init_op(new_op);
	new_op->ops_size = DOUBLE_ROTATE_OPS_SIZE;

	new_op->nodes[0] = pz;
	new_op->nodes[1] = z;
	new_op->nodes[2] = x;
	new_op->nodes[3] = y;

	new_op->ops[0] = oppz;
	new_op->ops[1] = opz;
	new_op->ops[2] = opx;
	new_op->ops[3] = opy;

	node_t* new_z = (node_t*) xmalloc(sizeof(node_t));
	node_t* new_x = (node_t*) xmalloc(sizeof(node_t));
	node_t* new_y = (node_t*) xmalloc(sizeof(node_t));
	if (left) {
		init_node(new_z, z->key, z->rank - 1, y->right, z->right, dummy);
		init_node(new_x, x->key, x->rank - 1, x->left, y->left, dummy);
		init_node(new_y, y->key, y->rank + 1, new_x, new_z, dummy);
	} else {
		init_node(new_z, z->key, z->rank - 1, z->left, y->left, dummy);
		init_node(new_x, x->key, x->rank - 1, y->right, x->right, dummy);
		init_node(new_y, y->key, y->rank + 1, new_z, new_x, dummy);
	}
	new_op->subtree = new_y;

	return new_op;
}

bool can_promote(const volatile node_t* pz, const volatile node_t* z,
		const volatile node_t* zs) {

	if (!pz || !z || !zs)
		return false;

	if (pz->rank == z->rank)
		return false;

	if (pz->rank == z->rank + 1 && pz->rank == zs->rank) {
		return false;
	}

	return true;
}

int height() {
	return height_node(root->left->left);
}

int height_node(volatile node_t* node) {
	if (!node) {
		return 0;
	} else {
		int left_height = height_node(node->left);
		int right_height = height_node(node->right);
		return left_height > right_height ? left_height + 1 : right_height + 1;
	}
}

void print_node(volatile node_t* node) {
	if (!node)
		return;
	printf("(key:%ld, rank:%ld)\n", node->key, node->rank);
}

void print_tree() {
	print_tree_node(root, 0);
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
