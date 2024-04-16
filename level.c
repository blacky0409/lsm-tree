#include "global.h"

Level *CreateLevel(int size, double fpr){
	Level *level = (Level *) malloc(sizeof(Level));
	if(level == NULL){
		printf("There is not enough memory for a new level.");
		return NULL;
	}
	level->size = size;
	level->count = 0;
	level->array = (Run *) malloc(size * sizeof(Run));
	if(level->array == NULL){
		printf("There is not enough memory for the array of runs.");
		return NULL;
	}
	level->targetfpr = fpr;
	return level;
}

//the structure of the level is a stack, 
//both insertion and deletion happen at the end of the array.

void InsertRun(LSMtree *lsm, Level *level, int count, int size, char * start, char * end){
	pthread_rwlock_wrlock(&lsm->level_lock);
	level->array[level->count].count = count;
	level->array[level->count].size = size;
	strcpy(level->array[level->count].start , start);
	strcpy(level->array[level->count].end , end);
	level->count += 1;
	pthread_rwlock_unlock(&lsm->level_lock);
}

Run PopRun(LSMtree *lsm,Level *level){
	pthread_rwlock_wrlock(&lsm->level_lock);
	Run run = level->array[level->count - 1];
	level->count -= 1;
	pthread_rwlock_unlock(&lsm->level_lock);
	return run;
}

void ClearLevel(Level *l){
	free(l->array);
	free(l);
}
