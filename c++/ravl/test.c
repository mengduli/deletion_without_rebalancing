/*
 * test.c
 *
 *  Created on: Nov 15, 2015
 *      Author: mengdu
 */
#include <getopt.h>
#include <signal.h>
#include <sys/time.h>
#include "dwrbavl.h"
#include "atomic_ops.h"
#include <unistd.h>
#include <stdlib.h>
#include "../common_ops.h"

#define DEFAULT_DURATION                1000
#define DEFAULT_INITIAL                 256
#define DEFAULT_NB_THREADS              32
#define DEFAULT_RANGE                   0x7FFFFFFF
#define DEFAULT_SEED                    0
#define DEFAULT_UPDATE                  20
#define DEFAULT_ELASTICITY              4
#define DEFAULT_ALTERNATE               0
#define DEFAULT_EFFECTIVE               0
#define DEFAULT_INSERT_RATIO			50
#define BLOCK_SIZE						1000
#define DEFAULT_PRESORTEDNESS			0
int key_dist = UNIFORM;
double alpha = 0;
int real_file = AOL;
unsigned long* query_from_file = NULL;
unsigned long query_size = 0;
unsigned long* uniq_query_from_file = NULL;
unsigned long uniq_query_size = 0;
/* presortedness initial keys and operations */
int presortedness = DEFAULT_PRESORTEDNESS;
int p_dup = 0;
char presortedness_file_name[20];
unsigned long* p_init_keys = NULL;
unsigned long p_init_keys_size = 0;
unsigned long* p_ops = NULL;
unsigned long p_ops_size = 0;

#define XSTR(s)                         STR(s)
#define STR(s)                          #s

//#define THROTTLE_NUM  1000
//#define THROTTLE_TIME 10000
//#define THROTTLE_MAINTENANCE

volatile AO_t stop;
unsigned int global_seed;
#ifdef TLS
__thread unsigned int *rng_seed;
#else /* ! TLS */
pthread_key_t rng_seed_key;
#endif /* ! TLS */
unsigned int levelmax;

void barrier_init(barrier_t *b, int n) {
	pthread_cond_init(&b->complete, NULL);
	pthread_mutex_init(&b->mutex, NULL);
	b->count = n;
	b->crossing = 0;
}

void barrier_cross(barrier_t *b) {
	pthread_mutex_lock(&b->mutex);
	/* One more thread through */
	b->crossing++;
	/* If not all here, wait */
	if (b->crossing < b->count) {
		pthread_cond_wait(&b->complete, &b->mutex);
	} else {
		pthread_cond_broadcast(&b->complete);
		/* Reset for next time */
		b->crossing = 0;
	}
	pthread_mutex_unlock(&b->mutex);
}

void *p_test(void* data) {

	thread_data_t *d = (thread_data_t *) data;
	double insert_ratio = (double)d->insert / 100 * (double)d->update / 100;
	unsigned long val = 0;
	double operation = 0;
	const gsl_rng_type* T;
	gsl_rng* r;
	gsl_rng_env_setup();
	T = gsl_rng_default;
	r = gsl_rng_alloc(T);
	int seed = d->seed;
	gsl_rng_set(r,seed);
	/* Wait on barrier */
	barrier_cross(d->barrier);
	int index = d->id;
	while (index < p_ops_size) {
		operation = gsl_rng_uniform(r);
		if (p_dup) {
			val = p_ops[index];
			if (operation < insert_ratio) {
				if(insert(val)) {
					d->nb_added++;
				}
				d->nb_add++;

			} else if (operation < (double) d->update / 100) {
				if(delete(val)) {
					d->nb_removed++;
				}
				d->nb_remove++;
			} else {
				if(get(val)) {
					d->nb_found++;
				}
				d->nb_contains++;
			}
		} else {
			if (operation < insert_ratio) {
				val = p_ops[index];
				if(insert(val)) {
					d->nb_added++;
				}
				d->nb_add++;

			} else if (operation < (double) d->update / 100) {
//				val = p_ops[(int)(index * gsl_rng_uniform(r))];
				val = rand_gsl(r, d->range, UNIFORM);
				if(delete(val)) {
					d->nb_removed++;
				}
				d->nb_remove++;
			} else {
//				val = p_ops[(int)(index * gsl_rng_uniform(r))];
				val = rand_gsl(r, d->range, UNIFORM);
				if(get(val)) {
					d->nb_found++;
				}
				d->nb_contains++;
			}
		}

		index += d->numThreads;
	}
	return NULL;
}

void *test(void *data) {
	thread_data_t *d = (thread_data_t *) data;
	double insert_ratio = (double)d->insert / 100 * (double)d->update / 100;
	unsigned long val = 0;
	double operation = 0;
	const gsl_rng_type* T;
	gsl_rng* r;
	gsl_rng_env_setup();
	T = gsl_rng_default;
	r = gsl_rng_alloc(T);
	int seed = d->seed;
	gsl_rng_set(r,seed);
	/* Wait on barrier */
	barrier_cross(d->barrier);
	int round = 0;
	int counter = 0;
	long real_data_index = 0;
	//#ifdef ICC
	while (stop == 0) {
		if (key_dist == REAL) {
			if (counter == 1000) {
				++round;
				counter = 0;
			}
			real_data_index = BLOCK_SIZE * (round * d->numThreads + d->id) + counter;
			if (real_data_index >= query_size) {
				real_data_index = 0;
				counter = 0;
				round = 0;
			}
			counter++;
		}
		//printf("counter %d round %d read_data_index: %ld\n", counter, round, real_data_index);
		val = (key_dist == REAL) ? query_from_file[real_data_index] : rand_gsl(r, d->range, key_dist);
		operation = gsl_rng_uniform(r);
		assert(val > 0);
		if(operation < insert_ratio) {
			if(insert(val)) {
				d->nb_added++;
			}
			d->nb_add++;

		} else if (operation < (double)d->update / 100) {
			if(delete(val)) {
				d->nb_removed++;
			}
			d->nb_remove++;
		} else {
			if(get(val)) {
				d->nb_found++;
			}
			d->nb_contains++;
		}
	}

	return NULL;
}

int main(int argc, char **argv) {
	struct option long_options[] = {
	// These options don't set a flag
			{ "help", no_argument, NULL, 'h' }, { "duration", required_argument,
			NULL, 'd' }, { "initial-size", required_argument, NULL, 'i' }, {
					"thread-num",
					required_argument, NULL, 't' }, { "range",
			required_argument, NULL, 'r' }, { "seed", required_argument,
			NULL, 'S' }, { "update-rate", required_argument, NULL, 'u' }, {
					"unit-tx",
					required_argument, NULL, 'x' },
					{ "zipf", required_argument, NULL, 'Z' },
					{ "real", required_argument, NULL, 'R' },
					{ "presortedness", required_argument, NULL, 'p' },
					{ "duplicate", required_argument, NULL, 'D' },
					{ "violations", required_argument, NULL, 'v' },
					{ NULL, 0, NULL, 0 } };

	node_t *set;
	//sl_intset_t *set;
	int i, c, size;
	unsigned long last = -1;
	unsigned long val = 0;
	unsigned long reads, effreads, updates, effupds, aborts, aborts_locked_read,
			aborts_locked_write, aborts_validate_read, aborts_validate_write,
			aborts_validate_commit, aborts_invalid_memory, max_retries;
	thread_data_t *data;
	pthread_t *threads;
	pthread_attr_t attr;
	barrier_t barrier;
	struct timeval start, end;
	struct timespec timeout;
	int duration = DEFAULT_DURATION;
	int initial = DEFAULT_INITIAL;
	int nb_threads = DEFAULT_NB_THREADS;
	unsigned long range = DEFAULT_RANGE;
	unsigned seed = DEFAULT_SEED;
	int update = DEFAULT_UPDATE;
	int insert_ratio = DEFAULT_INSERT_RATIO;
	int unit_tx = DEFAULT_ELASTICITY;
	int alternate = DEFAULT_ALTERNATE;
	int effective = DEFAULT_EFFECTIVE;
	sigset_t block_set;
	int num_of_violation = 0;

	while (1) {
		i = 0;
		c = getopt_long(argc, argv, "hAEGf:d:i:t:r:S:u:x:Z:R:p:D:v:", long_options, &i);
		if (c == -1)
			break;

		if (c == 0 && long_options[i].flag == 0)
			c = long_options[i].val;

		switch (c) {
		case 0:
			/* Flag is automatically set */
			break;
		case 'p':
			presortedness = 1;
			strcpy(presortedness_file_name, optarg);
			break;
		case 'D':
			p_dup = 1;
			break;
		case 'G':
			key_dist = GAUSSIAN;
			break;
		case 'Z':
			key_dist = ZIPF;
			alpha = atof(optarg);
			break;
		case 'R':
			key_dist = REAL;
			real_file = atoi(optarg);
			break;
		case 'v':
			num_of_violation = atoi(optarg);
			break;
		case 'h':
			printf(
					"Lock-Free BST stress test "
							"\n"
							"Usage:\n"
							"  intset [options...]\n"
							"\n"
							"Options:\n"
							"  -h, --help\n"
							"        Print this message\n"
							"  -A, --Alternate\n"
							"        Consecutive insert/remove target the same value\n"
							"  -f, --effective <int>\n"
							"        update txs must effectively write (0=trial, 1=effective, default=" XSTR(DEFAULT_EFFECTIVE) ")\n"
					"  -d, --duration <int>\n"
					"        Test duration in milliseconds (0=infinite, default=" XSTR(DEFAULT_DURATION) ")\n"
					"  -i, --initial-size <int>\n"
					"        Number of elements to insert before test (default=" XSTR(DEFAULT_INITIAL) ")\n"
					"  -t, --thread-num <int>\n"
					"        Number of threads (default=" XSTR(DEFAULT_NB_THREADS) ")\n"
					"  -r, --range <int>\n"
					"        Range of integer values inserted in set (default=" XSTR(DEFAULT_RANGE) ")\n"
					"  -S, --seed <int>\n"
					"        RNG seed (0=time-based, default=" XSTR(DEFAULT_SEED) ")\n"
					"  -u, --update-rate <int>\n"
					"        Percentage of update transactions (default=" XSTR(DEFAULT_UPDATE) ")\n"
					"  -x, --unit-tx (default=1)\n"
					"        Use unit transactions\n"
					"        0 = non-protected,\n"
					"        1 = normal transaction,\n"
					"        2 = read unit-tx,\n"
					"        3 = read/add unit-tx,\n"
					"        4 = read/add/rem unit-tx,\n"
					"        5 = all recursive unit-tx,\n"
					"        6 = harris lock-free\n");
			exit(0);
		case 'A':
			alternate = 1;
			break;
		case 'E':
			effective = 1;
			break;
		case 'f':
			insert_ratio = atoi(optarg);
			//effective = atoi(optarg);
			break;
		case 'd':
			duration = atoi(optarg);
			break;
		case 'i':
			initial = atoi(optarg);
			break;
		case 't':
			nb_threads = atoi(optarg);
			break;
		case 'r':
			range = atol(optarg);
			break;
		case 'S':
			seed = atoi(optarg);
			break;
		case 'u':
			update = atoi(optarg);
			break;
		case 'x':
			unit_tx = atoi(optarg);
			break;
		case '?':
			printf("Use -h or --help for help\n");
			exit(0);
		default:
			exit(1);
		}
	}

	assert(duration >= 0);
	assert(initial >= 0);
	assert(nb_threads > 0);
	assert(range > 0 && range >= initial);
	assert(update >= 0 && update <= 100);


	timeout.tv_sec = duration / 1000;
	timeout.tv_nsec = (duration % 1000) * 1000000;

	data = (thread_data_t *) xmalloc(nb_threads * sizeof(thread_data_t));
	threads = (pthread_t *) xmalloc(nb_threads * sizeof(pthread_t));

	init_tree(num_of_violation);

	i = 0;
	data[i].first = last;
	data[i].range = range;
	data[i].update = update;
	data[i].insert = insert_ratio;
	data[i].alternate = alternate;
	data[i].effective = effective;
	data[i].nb_add = 0;
	data[i].nb_added = 0;
	data[i].nb_remove = 0;
	data[i].nb_removed = 0;
	data[i].nb_contains = 0;
	data[i].nb_found = 0;
	data[i].barrier = &barrier;
	data[i].id = i;
	const gsl_rng_type* T;
	gsl_rng* r;
	gsl_rng_env_setup();
	T = gsl_rng_default;
	r = gsl_rng_alloc(T);
	gsl_rng_set(r,seed);

	if (key_dist == ZIPF) {
		initZipf(r, range, alpha);
	} else if (key_dist == REAL) {
		load_query_from_file(real_file, &query_from_file, &query_size,
				&uniq_query_from_file, &uniq_query_size, alternate);
	} else if (presortedness) {
		load_presortedness_from_file(presortedness_file_name, &p_ops, &p_ops_size, &p_init_keys, &p_init_keys_size);
	}

	/* Populate set */
	if (presortedness) {
		for (int i = 0; i < p_init_keys_size; ++i) {
			insert(p_init_keys[i]);
		}
	} else if (key_dist != REAL) {
		/* Populate set */
		i = 0;
		while (i < initial) {
			val = rand_gsl(r, range, key_dist);
			if (insert(val)) {
				last = val;
				i++;
			}
		}
	} else {
		initial = uniq_query_size / 2;
		for (int i = 0; i < initial; ++i) {
			insert(uniq_query_from_file[i]);
		}
	}
	size = data[0].nb_added + 2; /// Add 2 for the 2 sentinel keys
	//size = sl_set_size(set);

	/* Access set from all threads */
	barrier_init(&barrier, nb_threads + 1);
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	for (i = 0; i < nb_threads; i++) {
		data[i].first = last;
		data[i].range = range;
		data[i].update = update;
		data[i].insert = insert_ratio;
		data[i].alternate = alternate;
		data[i].effective = effective;
		data[i].nb_add = 0;
		data[i].nb_added = 0;
		data[i].nb_remove = 0;
		data[i].nb_removed = 0;
		data[i].nb_contains = 0;
		data[i].nb_found = 0;
		data[i].barrier = &barrier;
		data[i].id = i;
		data[i].seed = seed + i;
		data[i].numThreads = nb_threads;
		if (presortedness) {
			if (pthread_create(&threads[i], &attr, p_test, (void*)(&data[i]))
					!= 0) {
				perror("error creating thread");
				exit(1);
			}
		} else {
			if (pthread_create(&threads[i], &attr, test, (void *) (&data[i]))
					!= 0) {
				perror("error creating thread");
				exit(1);
			}
		}
	}
	pthread_attr_destroy(&attr);

	/* Start threads */
	barrier_cross(&barrier);

	gettimeofday(&start, NULL);
	if (!presortedness) {
	if (duration > 0) {
		nanosleep(&timeout, NULL);
	} else {
		sigemptyset(&block_set);
		sigsuspend(&block_set);
	}
	}
#ifdef ICC
	stop = 1;
#else
	AO_store_full(&stop, 1);
#endif /* ICC */



	/* Wait for thread completion */
	for (i = 0; i < nb_threads; i++) {
		if (pthread_join(threads[i], NULL) != 0) {
			perror("Error waiting for thread completion\n");
			exit(1);
		}
	}
	gettimeofday(&end, NULL);

	duration = (end.tv_sec * 1000 + end.tv_usec / 1000)
			- (start.tv_sec * 1000 + start.tv_usec / 1000);
	double p_duration = 0;
	if (presortedness) {
		p_duration = (double)end.tv_sec * 1000 -
				(double)start.tv_sec * 1000 +
				(double)end.tv_usec / 1000 -
				(double)start.tv_usec / 1000;
	}
	reads = 0;
	effreads = 0;
	updates = 0;
	effupds = 0;
	max_retries = 0;
	for (i = 0; i < nb_threads; i++) {
		reads += data[i].nb_contains;
		effreads += data[i].nb_contains + (data[i].nb_add - data[i].nb_added)
				+ (data[i].nb_remove - data[i].nb_removed);
		updates += (data[i].nb_add + data[i].nb_remove);
		effupds += data[i].nb_removed + data[i].nb_added;
		size += data[i].nb_added - data[i].nb_removed;

	}
//	print_tree();
	end:
	if (presortedness) {
		printf("dwrbavl%d,%s,%ld,%.2f,%.2f,%d,%.2f,%.2f,%.2f,%d\n",
				num_of_violation, presortedness_file_name, range,
			(double) update / 100,
			(double) insert_ratio / 100 * (double) update / 100, nb_threads,
			(double) effupds / (double) updates, p_duration,
			(reads + updates) * 1000.0 / p_duration, height());
	} else {
		printf("dwrbavl%d,%ld,%d,%.2f,%.2f,%d,(%d %.2f),%.2f,%d\n",
				num_of_violation, range, initial,
			(double)update / 100, (double)insert_ratio / 100 *
			(double)update / 100, nb_threads, key_dist, (double) effupds / (double) updates,
			(reads + updates) * 1000.0 / duration, height());
	}
	/* Delete set */
	//sl_set_delete(set);
#ifndef TLS
	pthread_key_delete(rng_seed_key);
#endif /* ! TLS */

	free(threads);
	free(data);

	return 0;
}

