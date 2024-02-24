#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <unistd.h>
#include <netdb.h> 
#include <netinet/in.h> 
#include <sys/socket.h> 
#include <sys/types.h>
#include <time.h>
#include <pthread.h>
#include <errno.h>
#include <unistd.h>

#define STRING_SIZE 10
#define MAX_VALUE_SIZE 10
#define MAX_PAGE 70000


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
	pthread_mutex_t lock;
} LSMtree;

typedef struct ValueLog{
	FILE *fp;
	int head;
	int tail;
} ValueLog;

typedef struct Save_Log{
	char key[STRING_SIZE];
	int key_len;
	int value;
} SaveLog;

typedef struct Save_Array{
	Node * array;
	int index;
	char filename[14];
	int size;
}SaveArray;
//heap.c
Heap *CreateHeap(int size);
int GetKeyPos(Heap *h, char * key);
void HeapifyBottomTop(Heap *h, int index);
void HeapifyTopBottom(Heap *h, int parent);
void InsertKey(Heap *h, char * key, int value, bool flag);
Node PopMin(Heap *h);
void PrintNode(Heap *h,ValueLog *log);
void ClearHeap(Heap *h);

//level.c
Level *CreateLevel(int size, double fpr);
void InsertRun(Level *level, int count, int size, char * start, char * end);
Run PopRun(Level *level);
void ClearLevel(Level *l);

//hashtable.c
HashTable *CreateHashTable(int size);
void AddToTable(HashTable *table, char * key);
bool CheckTable(HashTable *table, char * key);
void ClearTable(HashTable *table);

//lsm-tree.c
LSMtree *CreateLSM(int buffersize, int sizeratio, double fpr);
void Merge(LevelNode *Current, int origin, int levelsize,
	int runcount, int runsize, Node *sortedrun, double targetfpr);
void Put(LSMtree *lsm, char * key, int value, bool flag,ValueLog *log);
Node * Get_loc(LSMtree *lsm, char * key);
void Range(LSMtree *lsm, char * start, char * end, ValueLog *log);
void PrintStats(LSMtree *lsm,ValueLog *log);
int Get(LSMtree *lsm, char * key, ValueLog *log);
SaveArray * Get_array(LSMtree *lsm, char * key);

//value-log.c
ValueLog *CreateLog(int head, int tail);
void ValuePut(ValueLog *log, int *loc, const char * key, uint64_t key_len, uint64_t value);
uint64_t ValueGet(ValueLog *log, int loc);
void ClearLog(ValueLog *log);
int ValueLog_sync(ValueLog *log);
void GC(LSMtree *lsm,ValueLog *log);
