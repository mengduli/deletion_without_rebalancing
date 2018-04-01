/*
 * dwrbavl.h
 *
 *  Created on: Nov 15, 2015
 *      Author: mengdu
 */

#ifndef DWRBAVL_H_
#define DWRBAVL_H_

#include <pthread.h>
#include <stdio.h>
#include <jemalloc/jemalloc.h>

#define true 					1
#define false 					0

#define null					0

#define STATE_INPROGRESS		0
#define STATE_ABORTED			1
#define STATE_COMMITTED			2

#define SUCCESS 				0
#define MALLOC_ERR				1

#define INSERT_OPS_SIZE			2
#define REMOVE_OPS_SIZE			3
#define PROMOTE_OPS_SIZE		2
#define ROTATE_OPS_SIZE			3
#define DOUBLE_ROTATE_OPS_SIZE	4
#define MAX_OPS_SIZE			4

typedef struct barrier {
	pthread_cond_t complete;
	pthread_mutex_t mutex;
	int count;
	int crossing;
} barrier_t;

typedef struct thread_data {
  unsigned long first;
  unsigned long range;
  int update;
  int insert;
  int alternate;
  int effective;
  int id;
  unsigned long numThreads;
  unsigned long nb_add;
  unsigned long nb_added;
  unsigned long nb_remove;
  unsigned long nb_removed;
  unsigned long nb_contains;
  unsigned long nb_found;
  unsigned long ops;
  unsigned int seed;
  double search_frac;
  double insert_frac;
  double delete_frac;
  unsigned long keyspace1_size;
  barrier_t *barrier;

} thread_data_t;

inline void *xmalloc(size_t size) {
  void *p = malloc(size);
  if (p == NULL) {
    perror("malloc");
    exit(1);
  }
  return p;
}

struct node {
	volatile struct node* left;
	volatile struct node* right;
	unsigned long key;
	volatile struct operation* op;
	volatile bool marked;
	unsigned long rank;
};

struct operation {
	volatile struct node* nodes[MAX_OPS_SIZE];
	volatile struct operation* ops[MAX_OPS_SIZE];
	volatile struct node* subtree;
	volatile int state;
	volatile bool all_frozen;
	volatile int ops_size;
};

typedef struct node node_t;
typedef struct operation operation_t;

int init_node(node_t* node_ptr, const unsigned long key, const unsigned long rank, volatile node_t* left, volatile node_t* right,
		volatile operation_t* op);
bool is_sentinel(volatile node_t* node);
int init_dummy_op(volatile operation_t* op_ptr);
int init_op(operation_t* op_ptr);
void clear_op(volatile operation_t* op_ptr);

int init_tree(const int all_violation_per_path);
int tree_size();
bool get(const unsigned long key);
bool insert(const unsigned long key);
bool delete(const unsigned long key);
int sequential_size(volatile node_t* node);
volatile operation_t* weak_llx(volatile node_t* node_ptr);
bool weak_llx_array(volatile node_t* node_ptr, const int i,
		volatile operation_t** ops, volatile node_t** nodes);
bool help_scx(volatile operation_t* op, const int start_index);

void fix_to_key(const unsigned long key);
volatile operation_t* create_insert_operation(volatile node_t* p,
		volatile node_t* l, const unsigned long key);
volatile operation_t* create_remove_operation(volatile node_t* gp,
		volatile node_t* p, volatile node_t* l);
volatile operation_t* create_balancing_operation(volatile node_t* pz,
		volatile node_t* z, volatile node_t* x);
volatile operation_t* create_promote_op(volatile node_t* pz, volatile node_t* z,
		volatile operation_t* oppz,
		volatile operation_t* opz, const bool left);
volatile operation_t* create_rotate1_op(volatile node_t* pz, volatile node_t* z,
		volatile node_t* x, volatile operation_t* oppz,
		volatile operation_t* opz, volatile operation_t* opx, const bool left);
volatile operation_t* create_rotate2_op(volatile node_t* pz, volatile node_t* z,
		volatile node_t* x, volatile operation_t* oppz,
		volatile operation_t* opz, volatile operation_t* opx, const bool left);
volatile operation_t* create_double_rotate_op(volatile node_t* pz,
		volatile node_t* z, volatile node_t* x,
		volatile node_t* y, volatile operation_t* oppz,
		volatile operation_t* opz, volatile operation_t* opx, volatile operation_t* opy,
		const bool left);
bool can_promote(const volatile node_t* pz, const volatile node_t* z,
		const volatile node_t* zs);

int height();
int height_node(volatile node_t* node);
void print_node(volatile node_t* node);
void print_tree();
void print_tree_node();

typedef struct record record_t;
#endif /* DWRBAVL_H_ */
