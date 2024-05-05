#include "global.h"

#define LOC_SLOW "SlowMemory/"
#define TO_SLOW 1

ValueLog *CreateLog(int head, int tail){
	init_arenas();

	set_fast_node(0);
	set_slow_node(1);

	ValueLog * log = (ValueLog *) malloc(sizeof(ValueLog));
	FastMem *fast = (FastMem *)malloc(sizeof(FastMem));
	SlowMem *slow = (SlowMem *)malloc(sizeof(SlowMem));

	//fast
	log->fast = fast;
	log->fast->fp = fast_malloc(FAST_MAX_PAGE);
	log->fast->head = head;
	log->fast->tail = tail;
	log->fast->utili = 0;

	//slow
	log->slow = slow;
	log->slow->fp = slow_malloc(SLOW_MAX_PAGE);
	log->slow->head=head;

	pthread_mutex_init(&log->fast_lock,NULL);
	pthread_cond_init(&log->cond ,NULL);// PTHREAD_COND_INITIALIZER;
	return log;
}
void SlowPut(ValueLog *log, int *loc, const char *key, uint64_t key_len, uint64_t value){

	SaveLog *save = (SaveLog *)malloc(sizeof(SaveLog));

	save->key_len = key_len;
	strncpy(save->key,key,key_len);
	save->value = value;

	memcpy(log->slow->fp+SLOW_OFFSET(log->slow->head), save, sizeof(SaveLog));

	*loc = log->slow->head + (MAX_LOG_SIZE + 1);

	log->slow->head += sizeof(SaveLog);

//	free(save);	
}

int ValuePut(LSMtree *lsm,ValueLog *log, int *loc, const char * key, uint64_t key_len, uint64_t value,int flag){
	SaveLog *save = (SaveLog *)malloc(sizeof(SaveLog));

	save->key_len = key_len;
	strncpy(save->key,key,key_len);
	save->value = value;

	pthread_mutex_lock(&log->fast_lock);
	if(flag==1&& log->fast->utili >= FAST_MAX_PAGE - 10*sizeof(SaveLog)){
		pthread_cond_wait(&log->cond,&log->fast_lock);
	}
	
	memcpy(log->fast->fp + FAST_OFFSET(log->fast->head), save, sizeof(SaveLog));

	log->fast->utili += sizeof(SaveLog);
	*loc = log->fast->head;

	log->fast->head += sizeof(SaveLog);

	log->fast->head = log->fast->head % FAST_MAX_PAGE;


	pthread_mutex_unlock(&log->fast_lock);

//	free(save);	
	return 1;
}

uint64_t ValueGet(ValueLog *log,int loc){

	printf("location : %d\n",loc);
	if(loc == -1){
		printf("location is -1\n");
		return -1;
	}
	uint64_t value = -1;
	bool slow = false;

	if(loc >= (MAX_LOG_SIZE + 1)){
		loc -= (MAX_LOG_SIZE + 1);
		slow = true;
		printf("slow\n");
	}
	else{
		pthread_mutex_lock(&log->fast_lock);
		printf("fast\n");
	}
	if(!slow){
		if(log->fast->head > log->fast->tail && (log->fast->head <= loc || log->fast->tail > loc)){
			printf("fault 1 : %d\n",loc);
			pthread_mutex_unlock(&log->fast_lock);
			return value;
		}
		else if(log->fast->head < log->fast->tail && (log->fast->tail > loc && log->fast->head <=loc)){
			printf("fault 2\n");
			pthread_mutex_unlock(&log->fast_lock);
			return value;
		}

	}
	else{
		if(loc >= log->slow->head){
			printf("fault3\n");
			return value;
		}
	}

	SaveLog *save = (SaveLog *)malloc(sizeof(SaveLog));

	printf("offset : %ld\n",OFFSET(loc));
	if(!slow)
		memcpy(save,log->fast->fp + FAST_OFFSET(loc),sizeof(SaveLog));
	//	save = log->fast->fp[OFFSET(loc)];
	else
		memcpy(save,log->slow->fp + SLOW_OFFSET(loc),sizeof(SaveLog));
	//	save = log->slow->fp[OFFSET(loc)];

	value = save->value;
//	free(save);

	if(!slow)
		pthread_mutex_unlock(&log->fast_lock);

	return value;
}

void GC(LSMtree *lsm,ValueLog *log){
	int end = log->fast->head;	
	int size = 0;

	int tail = log->fast->tail;

	printf("ready GC\n");
	while(tail != end){
		SaveLog * save = (SaveLog *)malloc(sizeof(SaveLog));

		memcpy(save,log->fast->fp + FAST_OFFSET(tail),sizeof(SaveLog));


		char str_key[STRING_SIZE];
		strncpy(str_key,save->key,save->key_len);

		pthread_rwlock_wrlock(&lsm->merge_lock);
		SaveArray * dest = Get_array(lsm,str_key);

		if(dest != NULL){
			int loc = 0;
			if(dest->number <= TO_SLOW){
				ValuePut(lsm,log,&loc,str_key,save[size].key_len,save[size].value,0);
			}
			else{
				SlowPut(log,&loc,str_key,save[size].key_len,save[size].value);
			}
			log->fast->utili -= sizeof(SaveLog);
			dest->array[dest->index].value = loc;

			if(strcmp(dest->filename,"")!=0){
				pthread_rwlock_wrlock(&dest->level->level_lock);
				FILE *fp = fopen(dest->filename, "wt");
				if(fp == NULL)
					printf("fp error\n");
				fwrite(dest->array, sizeof(Node), dest->size, fp);
				fclose(fp);
				pthread_rwlock_unlock(&dest->level->level_lock);
			}
			else{
				pthread_rwlock_wrlock(&lsm->buffer_lock);
				memcpy(lsm->buffer->array,dest->array, dest->size * sizeof(Node));
				pthread_rwlock_unlock(&lsm->buffer_lock);
			}
		}
		else
			printf("Can't find key\n");
		dest = NULL;
		tail += sizeof(SaveLog);
		
		tail = tail % FAST_MAX_PAGE;

		log->fast->tail = tail;

		pthread_cond_signal(&log->cond);
		pthread_rwlock_unlock(&lsm->merge_lock);

	}
	pthread_cond_broadcast(&log->cond);
	printf("done\n");
}
void ClearLog(ValueLog *log){
	free(log);
}
