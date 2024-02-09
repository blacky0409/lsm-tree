#include "global.h"

Heap *CreateHeap(int size){
	Heap *h = (Heap *) malloc(sizeof(Heap));
	
	if(h == NULL){
		printf("There is not enough memory for heap.");
		return NULL;
	}
	
	h->size = size;
	h->count = 0;
	h->array = (Node *) malloc(size*sizeof(Node));

	if(h->array == NULL){
		printf("There is not engouth memory for the array of nodes.");
		return NULL;
	}
	return h;
}

int GetKeyPos(Heap *h, char * key){
	if(h->count == 0){
		return -1;
	}else{
		int i;
		for(i = 0; i < h->count; i++){
			if(strcmp(h->array[i].key , key)==0){
				return i;
			}
		}
		return -1;
	}
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
	int right = parent * 2 + 1;
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

void InsertKey(Heap *h, char * key, int value, bool flag){
	strcpy(key, h->array[h->count].key);
	h->array[h->count].value = value;
	h->array[h->count].flag = flag;
	HeapifyBottomTop(h, h->count);
	h->count += 1;
}

Node PopMin(Heap *h){
	Node pair;
	pair = h->array[0];
	h->array[0] = h->array[h->count - 1];
	h->count -= 1;
	HeapifyTopBottom(h, 0);
	return pair;
}

void PrintNode(Heap *h){
	int i;
	for(i=0; i<h->count; i++){
		printf("%s:%d:L0 ", h->array[i].key, h->array[i].value);
	}
	printf("\nThere are %d pairs on buffer. \n", h->count);
}

void ClearHeap(Heap *h){
	free(h->array);
	free(h);
}

