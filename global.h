#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <netdb.h> 
#include <netinet/in.h> 
#include <sys/socket.h> 
#include <sys/types.h>
#include <time.h>
#include <pthread.h>
#include <errno.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <numa.h>
#include <numaif.h>
#include <sched.h>
#include <FSmalloc.h>
#include <FSnuma.h>

#define STRING_SIZE 10
#define MAX_VALUE_SIZE 10

#define MAX_PAGE (4096) //usually fix
#define MAX_LOG_SIZE ( 4096 * 4096 * 4 ) //35

#define FAST_MAX_PAGE MAX_LOG_SIZE 
#define MAPPING_LOG_SIZE (4096) //usually fix
#define UTILIZATION (500)
#define SLOW_MAX_PAGE FAST_MAX_PAGE 

#define MAX_LOG_MAPPING ((int)(MAPPING_LOG_SIZE / sizeof(SaveLog))  * sizeof(SaveLog))

#define INDEX(X) (int)(X / MAPPING_LOG_SIZE)
#define FAST_OFFSET(X) (int)((X % FAST_MAX_PAGE) / sizeof(SaveLog))
#define SLOW_OFFSET(X) (int)((X % SLOW_MAX_PAGE) / sizeof(SaveLog))
#define OFFSET(X) (int)((X % MAPPING_LOG_SIZE) / sizeof(SaveLog))


#define	MAX_THREAD 32
#define LOC_FAST "FastMemory/"
#define FILE_NAME 25

extern int slow_intput;

typedef struct Node{
	char key[STRING_SIZE];
	int value;
	bool flag;
} Node;

typedef struct Heap{
	Node *array;
	int count;
	int size;
} Heap;

typedef struct Run{
	int count;
	int size;
	char start[STRING_SIZE];
	char end[STRING_SIZE];
} Run;

typedef struct Level{
	Run *array;
	int count;
	int size;
	double targetfpr;
	pthread_rwlock_t level_lock; //각 level에 input output을 진행할 때 거는 lock
	pthread_rwlock_t global_lock; //각 level에 input output을 진행할 때 거는 lock
} Level;

typedef struct LevelNode{
	Level *level;
	int number;
	struct LevelNode *next;
} LevelNode;

typedef struct HashTable{
	int count;
	char **array;
} HashTable;

typedef struct LSMtree{
	Heap *buffer;
	int T;
	LevelNode *L0;
	double fpr1;
	pthread_rwlock_t buffer_lock; //buffer에 거는 lock
	pthread_rwlock_t merge_lock; //GC가 일어날때 lsm tree의 구조 변화를 막기 위해 merge 불가 상태로 만드는 lock
	pthread_mutex_t make_level_lock; //level를 만들떄 임시로 거는 lock
} LSMtree;
typedef struct Save_Log{
	char key[STRING_SIZE];
	int key_len;
	int value;
} SaveLog;

typedef struct FastMem{
	SaveLog * fp;
	int head;
	int tail;
	int utili;
}FastMem;

typedef struct SlowMem{
	SaveLog * fp;
	int head;
}SlowMem;

typedef struct ValueLog{
	FastMem *fast;
	SlowMem *slow;
	pthread_mutex_t fast_lock;
	pthread_cond_t cond;
} ValueLog;

typedef struct Save_Array{
	Node * array;
	int number;
	int index;
	char filename[FILE_NAME];
	int size;
	Level *level;
}SaveArray;

typedef struct Element{
	char key[STRING_SIZE];
	int loc;
}Element;

typedef struct Queue{
	int front;
	int rear;
	int size;
	Element **array;
} Queue;

typedef struct TakeArg{
	Queue *q;
	ValueLog *log;
	bool finish;
}TakeArg;

typedef struct Merge_Arg{
	Node * array;
	LSMtree *lsm;
}Merge_Arg;

//heap.c
Heap *CreateHeap(int size);
int GetKeyPos(LSMtree *lsm, char * key);
void HeapifyBottomTop(Heap *h, int index);
void HeapifyTopBottom(Heap *h, int parent);
void InsertKey(LSMtree *lsm, char * key, int value, bool flag,int locking);
Node PopMin(LSMtree * lsm);
void PrintNode(LSMtree *lsm,ValueLog *log);
void ClearHeap(Heap *h);

//level.c
Level *CreateLevel(int size, double fpr);
void InsertRun(LSMtree *lsm,Level *level, int count, int size, char * start, char * end);
Run PopRun(LSMtree *lsm,Level *level);
void ClearLevel(Level *l);

//hashtable.c
HashTable *CreateHashTable(int size);
void AddToTable(HashTable *table, char * key);
bool CheckTable(HashTable *table, char * key);
void ClearTable(HashTable *table);
void *get_log(void *argument);
//lsm-tree.c
LSMtree *CreateLSM(int buffersize, int sizeratio, double fpr);
void Merge(LSMtree *lsm, LevelNode *Current, int origin, int levelsize,
	int runcount, int runsize, Node *sortedrun, double targetfpr);
void Put(LSMtree *lsm, char * key, int value, bool flag,ValueLog *log);
Node * Get_loc(LSMtree *lsm, char * key);
void Range(LSMtree *lsm, char * start, char * end, ValueLog *log);
void PrintStats(LSMtree *lsm,ValueLog *log);
int Get(LSMtree *lsm, char * key, ValueLog *log);
SaveArray * Get_array(LSMtree *lsm, char * key);
void Delete(LSMtree *lsm, char * key);

//value-log.c
ValueLog *CreateLog(int head, int tail);
int ValuePut(LSMtree *lsm,ValueLog *log, int *loc, const char * key, uint64_t key_len, uint64_t value,int flag);
uint64_t ValueGet(ValueLog *log, int loc);
void ClearLog(ValueLog *log);
void GC(LSMtree *lsm,ValueLog *log);

//queue.c
Queue *CreateQueue(int size);
bool is_empty(Queue *queue);
bool is_full(Queue *queue);
Element * GetToQueue(Queue *queue);
void AddToQueue(Queue *queue,char *key, int loc);
void ClearQueue(Queue *queue);
