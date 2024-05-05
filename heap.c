#include "global.h"

Heap *CreateHeap(int size){
	init_arenas();
	set_fast_node(0);
	set_slow_node(1);
	Heap *h = fast_malloc(sizeof(Heap));
	
	if(h == NULL){
		printf("There is not enough memory for heap.");
		return NULL;
	}
	
	h->size = size;
	h->count = 0;
	h->array = fast_malloc(size*sizeof(Node));

	if(h->array == NULL){
		printf("There is not engouth memory for the array of nodes.");
		return NULL;
	}
	return h;
}

int GetKeyPos(LSMtree *lsm, char * key){
	pthread_rwlock_rdlock(&lsm->buffer_lock);
	Heap *h = lsm->buffer;
	int re = -1;
	if(h->count == 0){
		goto GetKeyPosEnd;
	}else{
		int i;
		for(i = 0; i < h->count; i++){
			if(strcmp(h->array[i].key , key)==0){
				re = i;
				goto GetKeyPosEnd;
			}
		}
		goto GetKeyPosEnd;
	}
GetKeyPosEnd:
	pthread_rwlock_unlock(&lsm->buffer_lock);
	return re;
}

void HeapifyBottomTop(Heap *h, int index){
	Node temp;
	int parent = (index-1) / 2;
	if(strcmp(h->array[parent].key , h->array[index].key) > 0 ){
		temp = h->array[parent];
		h->array[parent] = h->array[index];
		h->array[index] = temp;
		HeapifyBottomTop(h, parent);
	}
}

void HeapifyTopBottom(Heap *h, int parent){
	int left = parent * 2 + 1;
	int right = parent * 2 + 2;
	int min;
	Node temp;
	if(left >= h->count){
		left = -1;
	}
	if(right >= h->count){
		right = -1;
	}
	if((left > 0) && (strcmp(h->array[left].key , h->array[parent].key) < 0 )){
		min = left;
	}else{
		min = parent;
	}
	if((right > 0) && (strcmp(h->array[right].key , h->array[min].key) < 0 )){
		min = right;
	}
	if(min != parent){
		temp = h->array[min];
		h->array[min] = h->array[parent];
		h->array[parent] = temp;
		HeapifyTopBottom(h, min);
	}
}

void InsertKey(LSMtree *lsm, char * key, int value, bool flag,int locking){
	if(locking)
		pthread_rwlock_wrlock(&lsm->buffer_lock);
	Heap * h = lsm->buffer;
	strcpy(h->array[h->count].key,key);
	h->array[h->count].value = value;
	h->array[h->count].flag = flag;
	HeapifyBottomTop(h, h->count);
	h->count += 1;
	if(locking)
		pthread_rwlock_unlock(&lsm->buffer_lock);
}

Node PopMin(LSMtree *lsm){
	Heap *h = lsm->buffer;
	Node pair;
	pair = h->array[0];
	h->array[0] = h->array[h->count - 1];
	h->count -= 1;
	HeapifyTopBottom(h, 0);
	return pair;
}

void PrintNode(LSMtree * lsm, ValueLog *log){
	Heap *h = lsm->buffer;
	int i;
	for(i=0; i<h->count; i++){
		printf("%s:%ld:L0 ", h->array[i].key, ValueGet(log,h->array[i].value));
	}
	printf("\nThere are %d pairs on buffer. \n", h->count);
}

void ClearHeap(Heap *h){
	free(h->array);
	free(h);
}

