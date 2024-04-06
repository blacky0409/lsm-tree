#include "global.h"

Queue *CreateQueue(int size){
	Queue *queue = (Queue *) malloc(sizeof(Queue));
	if(queue == NULL){
		printf("There is not enough memory for a hash table.");
		return NULL;
	}
	queue->front = 0;
	queue->rear = 0;
	queue->size = size;
	queue->array = (Element **) malloc(size * sizeof(Element *));
	pthread_mutex_init(&queue->lock,NULL);
	
	if(queue->array == NULL){
		printf("There is not enough memory for an array in the hash table.");
	}

	return queue;
}

bool is_empty(Queue *queue){
	if(queue->front == queue->rear){ 
		return true;
	}
	else{
		return false;
	}
}
bool is_full(Queue *queue){
	if(((queue->rear + 1) % queue->size) == queue->front){
		return true;
	}
	else{
		return false;
	}
}

Element * GetToQueue(Queue *queue){
	pthread_mutex_lock(&queue->lock);
	if(is_empty(queue)){
		pthread_mutex_unlock(&queue->lock);
//		printf("queue is empty\n");
		return NULL;
	}
	Element * re = queue->array[queue->front];
	queue->front = (queue->front + 1) % (queue->size);
	pthread_mutex_unlock(&queue->lock);
	return re;
}

void AddToQueue(Queue *queue,char *key, int loc){
	pthread_mutex_lock(&queue->lock);
	if(is_full(queue)){
		printf("queue is full\n");
		pthread_mutex_unlock(&queue->lock);
		return;
	}
	Element *ele = (Element *)malloc(sizeof(Element));
	ele->loc = loc;
	strcpy(ele->key,key);
	queue->array[queue->rear] = ele;
	queue->rear = (queue->rear + 1) % (queue->size);
	pthread_mutex_unlock(&queue->lock);
	return;
}

void ClearQueue(Queue *queue){
	free(queue->array);
	free(queue);
	pthread_mutex_destroy(&queue->lock);
}

