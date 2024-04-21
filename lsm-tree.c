#include "global.h"

#define INT_MAX 2147483647

int dead = 0;

LSMtree *CreateLSM(int buffersize, int sizeratio, double fpr){
	LSMtree *lsm = (LSMtree *) malloc(sizeof(LSMtree));
	if(lsm == NULL){
		printf("There is not enough memory for an LSM-tree.");
		return NULL;
	}
	lsm->buffer = CreateHeap(buffersize);
	lsm->T = sizeratio;
	lsm->L0 = (LevelNode *) malloc(sizeof(LevelNode));
	lsm->L0->level = NULL;
	lsm->L0->number = 0;
	lsm->L0->next = NULL;
	lsm->fpr1 = fpr; //delete
	pthread_rwlock_init(&lsm->buffer_lock,NULL);
	pthread_rwlock_init(&lsm->merge_lock,NULL);
	return lsm;
}

void * Merge_routin(void * arg){
	Merge_Arg *ar = (Merge_Arg *)arg;
	LSMtree *lsm = ar->lsm;
	Node * sortedrun = ar->array;

	Merge(lsm,lsm->L0, 0, (lsm->T - 1), 
			lsm->buffer->size, lsm->buffer->size, sortedrun, lsm->fpr1);
}

void Put(LSMtree *lsm, char * key, int value, bool flag,ValueLog *log){

	pthread_rwlock_rdlock(&lsm->merge_lock);
	int loc;
	ValuePut(lsm,log,&loc, key, strlen(key) + 1 , value);
	int position = GetKeyPos(lsm, key);
	if(position >= 0){
		pthread_rwlock_wrlock(&lsm->buffer_lock);
		lsm->buffer->array[position].value = loc;
		lsm->buffer->array[position].flag = flag;
		pthread_rwlock_unlock(&lsm->buffer_lock);
	}else{
	//	pthread_rwlock_wrlock(&lsm->buffer_lock);
		if(lsm->buffer->count < lsm->buffer->size){
			InsertKey(lsm, key, loc, flag);
		}else if(lsm->buffer->count == lsm->buffer->size){
			int i;
			Node *sortedrun = (Node *) malloc(lsm->buffer->size * sizeof(Node));
			for(i = 0; i < lsm->buffer->size; i++){
				sortedrun[i] = PopMin(lsm);
			}

//			Merge_Arg * arg = (Merge_Arg *)malloc(sizeof(Merge_Arg));
//			arg->array = sortedrun;
//			arg->lsm = lsm;
//			pthread_t thread;

//			pthread_create(&thread,NULL,Merge_routin,arg);
			Merge(lsm,lsm->L0, 0, (lsm->T - 1), 
				lsm->buffer->size, lsm->buffer->size, sortedrun, lsm->fpr1); //merge때, memmory의 값이 L1으로 이동된다면 그때 unlock해주기!

			InsertKey(lsm, key, loc, flag);
		}
	}
	pthread_rwlock_unlock(&lsm->merge_lock);
}

SaveArray * Get_array(LSMtree *lsm, char * key){
	int position = GetKeyPos(lsm, key);
	int i;
	SaveArray * save = (SaveArray *)malloc(sizeof(SaveArray));
	save->index = -1;
	save->number = -1;
	strcpy(save->filename,"");
	if(position != -1){
		if(lsm->buffer->array[position].flag){
			pthread_rwlock_rdlock(&lsm->buffer_lock);
			save->array = (Node *) malloc(lsm->buffer->size*sizeof(Node));
			memcpy(save->array,lsm->buffer->array, lsm->buffer->size * sizeof(Node));
			save->index=position;
			save->size=lsm->buffer->size;
			save->number=0;
			pthread_rwlock_unlock(&lsm->buffer_lock);
			return save;
		}
	}else{
		LevelNode *current = lsm->L0->next;
		while(current != NULL){
			Level *exploringlevel = current->level;
			pthread_rwlock_rdlock(&exploringlevel->level_lock);
			for(i = 0; i < exploringlevel->count; i++){
				if(strcmp(exploringlevel->array[i].start , key) <= 0 || strcmp(exploringlevel->array[i].end , key) >= 0){
					Node *currentarray = (Node *) malloc(exploringlevel->array[i].count * sizeof(Node));
					char filename[FILE_NAME];
					sprintf(filename, LOC_FAST"data/L%dN%d", current->number, i);
					FILE *fp = fopen(filename, "rt");
					fread(currentarray, sizeof(Node), exploringlevel->array[i].count, fp);
					fclose(fp);
					int left = 0;
					int right = exploringlevel->array[i].count - 1;
					int mid = (left + right) / 2;
					if(strcmp(key , currentarray[left].key) == 0){
						if(currentarray[left].flag){
							save->array = (Node *) malloc(exploringlevel->array[i].count * sizeof(Node));
							memcpy(save->array , currentarray,exploringlevel->array[i].count * sizeof(Node));
							save->index = left;
							strcpy(save->filename,filename);
							save->size = exploringlevel->array[i].count;
							save->number = current->number;
							save->level = exploringlevel;
							pthread_rwlock_unlock(&exploringlevel->level_lock);
							return save;
						}else{
							pthread_rwlock_unlock(&exploringlevel->level_lock);
							return NULL;
						}
					}else if(strcmp( key , currentarray[right].key) == 0){
						if(currentarray[right].flag){
							save->array = (Node *) malloc(exploringlevel->array[i].count * sizeof(Node));
							memcpy(save->array , currentarray,exploringlevel->array[i].count * sizeof(Node));
							save->index=right;
							strcpy(save->filename,filename);
							save->size = exploringlevel->array[i].count;
							save->number = current->number;
							save->level = exploringlevel;
							pthread_rwlock_unlock(&exploringlevel->level_lock);
							return save;
						}else{
							pthread_rwlock_unlock(&exploringlevel->level_lock);
							return NULL;
						}
					}
					while(left != mid){
						if(strcmp( key , currentarray[mid].key) == 0){
							if(currentarray[mid].flag){
								save->array = (Node *) malloc(exploringlevel->array[i].count * sizeof(Node));
								memcpy(save->array , currentarray,exploringlevel->array[i].count * sizeof(Node));
								save->index = mid;
								strcpy(save->filename,filename);
								save->size = exploringlevel->array[i].count;
								save->number = current->number;
								save->level = exploringlevel;
								pthread_rwlock_unlock(&exploringlevel->level_lock);
								return save;
							}else{
								pthread_rwlock_unlock(&exploringlevel->level_lock);
								return NULL;
							}
						}else if(strcmp ( key , currentarray[mid].key) > 0){
							left = mid;
							mid = (left + right) / 2;
						}else{
							right = mid;
							mid = (left + right) / 2;
						}
					}
					free(currentarray);	

				}
			}
			pthread_rwlock_unlock(&exploringlevel->level_lock);
			current = current->next;
		}
	}
	return NULL;
}

Node * Get_loc(LSMtree *lsm, char * key){
	int position = GetKeyPos(lsm, key);
	int i;
	if(position != -1){
		if(lsm->buffer->array[position].flag){
			pthread_rwlock_rdlock(&lsm->buffer_lock);
			Node * v = &lsm->buffer->array[position];
			pthread_rwlock_unlock(&lsm->buffer_lock);
			return v;
		}
	}else{
		LevelNode *current = lsm->L0->next;
		while(current != NULL){
			Level *exploringlevel = current->level;
			pthread_rwlock_rdlock(&exploringlevel->level_lock);
			for(i = 0; i < exploringlevel->count; i++){
				if(strcmp(exploringlevel->array[i].start , key) <= 0 || strcmp(exploringlevel->array[i].end , key) >= 0){
					Node *currentarray = (Node *) malloc(exploringlevel->array[i].count * sizeof(Node));
					char filename[FILE_NAME];
					sprintf(filename, LOC_FAST"data/L%dN%d", current->number, i);
					FILE *fp = fopen(filename, "rt");
					fread(currentarray, sizeof(Node), exploringlevel->array[i].count, fp);
					fclose(fp);
					int left = 0;
					int right = exploringlevel->array[i].count - 1;
					int mid = (left + right) / 2;
					if(strcmp(key , currentarray[left].key) == 0){
						if(currentarray[left].flag){
							Node * buffer = &currentarray[left];
							pthread_rwlock_unlock(&exploringlevel->level_lock);
							return buffer;
						}else{
							pthread_rwlock_unlock(&exploringlevel->level_lock);
							return NULL;
						}
					}else if(strcmp( key , currentarray[right].key) == 0){
						if(currentarray[right].flag){
							Node * buffer = &currentarray[right];
							pthread_rwlock_unlock(&exploringlevel->level_lock);
							return buffer;
						}else{
							pthread_rwlock_unlock(&exploringlevel->level_lock);
							return NULL;
						}
					}
					while(left != mid){
						if(strcmp( key , currentarray[mid].key) == 0){
							if(currentarray[mid].flag){
								Node * buffer = &currentarray[mid];
								pthread_rwlock_unlock(&exploringlevel->level_lock);
								return buffer;
							}else{
								pthread_rwlock_unlock(&exploringlevel->level_lock);
								return NULL;
							}
						}else if(strcmp ( key , currentarray[mid].key) > 0){
							left = mid;
							mid = (left + right) / 2;
						}else{
							right = mid;
							mid = (left + right) / 2;
						}
					}
					free(currentarray);	

				}
			}
			pthread_rwlock_unlock(&exploringlevel->level_lock);
			current = current->next;
		}
	}
	return NULL;
}
// 이건 나머지 성공하면 구현!
void Delete(LSMtree *lsm, char * key){

	int position = GetKeyPos(lsm, key);
	int i;
	if(position != -1){
		if(lsm->buffer->array[position].flag){
			lsm->buffer->array[position].flag = false;
			return;
		}
	}else{
		LevelNode *current = lsm->L0->next;
		while(current != NULL){
			Level *exploringlevel = current->level;
			for(i = 0; i < exploringlevel->count; i++){
				if(strcmp(exploringlevel->array[i].start , key) <= 0 && strcmp(exploringlevel->array[i].end , key) >= 0){
					Node *currentarray = (Node *) malloc(exploringlevel->array[i].count * sizeof(Node));
					char filename[FILE_NAME];
					sprintf(filename, LOC_FAST"data/L%dN%d", current->number, i);
					FILE *fp = fopen(filename, "rt");
					fread(currentarray, sizeof(Node), exploringlevel->array[i].count, fp);
					int left = 0;
					int right = exploringlevel->array[i].count - 1;
					int mid = (left + right) / 2;
					if(strcmp(key , currentarray[left].key) == 0){
						if(currentarray[left].flag){
							currentarray[left].flag = false;
						}
					}else if(strcmp( key , currentarray[right].key) == 0){
						if(currentarray[right].flag){
							currentarray[right].flag = false;
						}
					}
					while(left != mid){
						if(strcmp( key , currentarray[mid].key) == 0){
							if(currentarray[mid].flag){
								currentarray[mid].flag = false;
							}
						}else if(strcmp ( key , currentarray[mid].key) > 0){
							left = mid;
							mid = (left + right) / 2;
						}else{
							right = mid;
							mid = (left + right) / 2;
						}
					}
					fwrite(currentarray, sizeof(Node), exploringlevel->array[i].count, fp);
					fclose(fp);
					free(currentarray);						

				}
			}
			current = current->next;
		}
	}
	return;
}
void *get_log(void *argument){
	TakeArg * arg = (TakeArg *)argument;
	while(!arg->finish){
		Element *element = GetToQueue(arg->q);
		if(element == NULL)
			continue;
		else{
			printf("%s:%ld ",element->key,ValueGet(arg->log,element->loc));
		}
	}
	return NULL;
}

void Range(LSMtree *lsm, char * start, char * end,ValueLog *log){

	printf("start range\n");
	int i;
	int j;
	HashTable *table = CreateHashTable(128);
	Queue *q = CreateQueue(1024);
	pthread_t thread[MAX_THREAD];
	void *result;
	TakeArg *arg = (TakeArg *)malloc(sizeof(TakeArg));
	arg->q = q;
	arg->log = log;
	arg->finish = false;

	for(int i = 0 ; i < (sizeof(thread) / sizeof(thread[0])); i++){
		int r = pthread_create(&thread[i],NULL, get_log,arg);
		if(r == -1){
			printf("falut create thread\n");
			return;
		}
	}

	printf("range query result for [%s, %s] is ", start, end);
	for(i = 0; i < lsm->buffer->count; i++){
		if((strcmp(lsm->buffer->array[i].key , start) >= 0) && (strcmp(lsm->buffer->array[i].key , end)) < 0){
			if(!CheckTable(table, lsm->buffer->array[i].key)){
				AddToTable(table, lsm->buffer->array[i].key);
				if(lsm->buffer->array[i].flag){
					AddToQueue(q,lsm->buffer->array[i].key,lsm->buffer->array[i].value);
				}
			}
		}
	}
	LevelNode *currentlevelnode = lsm->L0->next;
	while(currentlevelnode != NULL){
		int levelnum = currentlevelnode->number;
		for(i = 0; i < currentlevelnode->level->count; i++){
			if((strcmp(currentlevelnode->level->array[i].end , start) >= 0) || (strcmp(currentlevelnode->level->array[i].start , end) <= 0)){
				Node *currentarray = (Node *) malloc(currentlevelnode->level->array[i].count * sizeof(Node));
				char filename[FILE_NAME];
				sprintf(filename, LOC_FAST"data/L%dN%d", levelnum, i);
				FILE *fp = fopen(filename, "rt");
				fread(currentarray, sizeof(Node), currentlevelnode->level->array[i].count, fp);
				fclose(fp);
				for(j = 0; j < currentlevelnode->level->array[i].count; j++){
					if(strcmp(currentarray[j].key , end) >= 0){
						continue;
					}else if(strcmp(currentarray[j].key , start) >= 0){
						if(!CheckTable(table, currentarray[j].key)){
							AddToTable(table, currentarray[j].key);
							if(currentarray[j].flag){
								AddToQueue(q,currentarray[j].key,currentarray[j].value);
							}
						}
					}
				}
				free(currentarray);
			}
		}
		currentlevelnode = currentlevelnode->next;
	}
	arg->finish = true;
	for (int i = 0; i < (sizeof(thread) / sizeof(thread[0])); i++) {
		int r = pthread_join(thread[i], &result);
		if(r == -1){
			printf("falut wait exit thread\n");
			return;
		}
	}
	printf("\n\n");
	ClearQueue(q);
	ClearTable(table);
	free(arg);
}


void PrintStats(LSMtree *lsm, ValueLog *log){
	int i;
	int j;
	int total = 0;

	LevelNode *Current = lsm->L0;
	LevelNode *currentlevelnode = Current->next;

	while(currentlevelnode != NULL){
		int levelnum = currentlevelnode->number;
		int currentcount = 0;
		for(i = 0; i < currentlevelnode->level->count; i++){
			currentcount += currentlevelnode->level->array[i].count;
			Node *currentarray = (Node *) malloc(currentlevelnode->level->array[i].count * sizeof(Node));
			char filename[FILE_NAME];
			sprintf(filename, LOC_FAST"data/L%dN%d", levelnum, i);
			FILE *fp = fopen(filename, "rt");
			fread(currentarray, sizeof(Node), currentlevelnode->level->array[i].count, fp);
			fclose(fp);
			for(j = 0; j < currentlevelnode->level->array[i].count; j++){
				printf("%s:%ld:L%d  ", currentarray[j].key, ValueGet(log,currentarray[j].value), levelnum);
				if(!currentarray[j].flag){
					total -= 1;
				}
			}
			printf("a run has ended. \n");
			free(currentarray);
		}
		printf("There are %d pairs on Level %d. \n\n", currentcount, levelnum);
		total += currentcount;
		currentlevelnode = currentlevelnode->next;
	}
	printf("There are %d pairs on the LSM-tree in total. \n", total);
}

int Get(LSMtree *lsm, char * key, ValueLog *log){	
	Node * dest = Get_loc(lsm,key);
	if(dest == NULL){
		printf("Don't find key\n");
		return -1;
	}
	printf("value %d\n",dest->value);
	int re = (ValueGet(log,dest->value));
	return re;
}

