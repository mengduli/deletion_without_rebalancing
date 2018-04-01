/*
 * chromatic.h
 *
 *  Created on: Nov 17, 2015
 *      Author: mengdu
 */

#ifndef CHROMATIC_H_
#define CHROMATIC_H_

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
#define BLK_OPS_SIZE			4
#define RB1_OPS_SIZE			3
#define RB2_OPS_SIZE			4
#define RB1SYM_OPS_SIZE			3
#define RB2SYM_OPS_SIZE			4
#define W1_OPS_SIZE				5
#define W2_OPS_SIZE				5
#define W3_OPS_SIZE				6
#define W4_OPS_SIZE				6
#define W5_OPS_SIZE				5
#define W6_OPS_SIZE				5
#define W7_OPS_SIZE				4
#define W1SYM_OPS_SIZE			5
#define W2SYM_OPS_SIZE			5
#define W3SYM_OPS_SIZE			6
#define W4SYM_OPS_SIZE			6
#define W5SYM_OPS_SIZE			5
#define W6SYM_OPS_SIZE			5
#define W7SYM_OPS_SIZE			4
#define PUSHUP_OPS_SIZE			4
#define PUSHUPSYM_OPS_SIZE		4
#define MAX_OPS_SIZE			6

typedef struct barrier {
	pthread_cond_t complete;
	pthread_mutex_t mutex;
	int count;
	int crossing;
} barrier_t;

typedef struct thread_data {
  unsigned long first;
  int insert;
  unsigned long range;
  int update;
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
	unsigned long weight;
	volatile bool marked;
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

int init_node(node_t* node_ptr, const unsigned long key,
		const unsigned long weight, volatile node_t* left, volatile node_t* right,
		volatile operation_t* op);
bool is_sentinel(volatile node_t* node);
bool has_child(volatile node_t* p, volatile node_t* c);
void print_node(volatile node_t* node);

int init_dummy_op(volatile operation_t* op_ptr);
int init_op(operation_t* op_ptr);
void clear_op(volatile operation_t* op_ptr);

volatile operation_t* weak_llx(volatile node_t* node_ptr);
bool weak_llx_array(volatile node_t* node_ptr, const int i,
		volatile operation_t** ops, volatile node_t** nodes);
bool help_scx(volatile operation_t* op, const int start_index);

int init_tree(const int all_violation_per_path);
int tree_size();
bool get(const unsigned long key);
bool insert(const unsigned long key);
bool delete(const unsigned long key);
void print_tree();

int sequential_size(volatile node_t* node);
void print_tree_node(volatile node_t* node, const int level);
void fix_to_key(const unsigned long key);
volatile operation_t* create_insert_operation(volatile node_t* p,
		volatile node_t* l, const unsigned long key);
volatile operation_t* create_remove_operation(volatile node_t* gp,
		volatile node_t* p, volatile node_t* l);

volatile operation_t* create_balancing_operation(volatile node_t* f,
		volatile node_t* fX, volatile node_t* fXX, volatile node_t* fXXX);
volatile operation_t* create_overweight_left_op(volatile node_t* f,
		volatile node_t* fX, volatile node_t* fXX, volatile node_t* fXXL,
		volatile operation_t* opf, volatile operation_t* opfX,
		volatile operation_t* opfXX, volatile operation_t* opfXXL,
		volatile node_t* fXL, volatile node_t* fXR, volatile node_t* fXXR,
		const bool fXXlef);
volatile operation_t* create_overweight_right_op(volatile node_t* f,
		volatile node_t* fX, volatile node_t* fXX, volatile node_t* fXXR,
		volatile operation_t* opf, volatile operation_t* opfX,
		volatile operation_t* opfXX, volatile operation_t* opfXXR,
		volatile node_t* fXR, volatile node_t* fXL, volatile node_t* fXXL,
		const bool fXXright);

volatile operation_t* createBlkOp(volatile operation_t* new_op);
volatile operation_t* createRb1Op(volatile operation_t* new_op);
volatile operation_t* createRb2Op(volatile operation_t* new_op);
volatile operation_t* createRb1SymOp(volatile operation_t* new_op);
volatile operation_t* createRb2SymOp(volatile operation_t* new_op);
volatile operation_t* createW1Op(volatile operation_t* new_op);
volatile operation_t* createW2Op(volatile operation_t* new_op);
volatile operation_t* createW3Op(volatile operation_t* new_op);
volatile operation_t* createW4Op(volatile operation_t* new_op);
volatile operation_t* createW5Op(volatile operation_t* new_op);
volatile operation_t* createW6Op(volatile operation_t* new_op);
volatile operation_t* createW7Op(volatile operation_t* new_op);
volatile operation_t* createW1SymOp(volatile operation_t* new_op);
volatile operation_t* createW2SymOp(volatile operation_t* new_op);
volatile operation_t* createW3SymOp(volatile operation_t* new_op);
volatile operation_t* createW4SymOp(volatile operation_t* new_op);
volatile operation_t* createW5SymOp(volatile operation_t* new_op);
volatile operation_t* createW6SymOp(volatile operation_t* new_op);
volatile operation_t* createW7SymOp(volatile operation_t* new_op);
volatile operation_t* createPushOp(volatile operation_t* new_op);
volatile operation_t* createPushSymOp(volatile operation_t* new_op);

int height();
int height_node(volatile node_t* node);

#endif /* CHROMATIC_H_ */
