#include "global.h"

#define LOC_SLOW "SlowMemory/"
#define TO_SLOW 1

ValueLog *CreateLog(int head, int tail){
	int fastfp = open(LOC_FAST"log", O_RDWR | O_CREAT | O_TRUNC | O_APPEND);
	if(fastfp  < 0){
		close(fastfp);
		printf("fp open error\n");
	}

	int slowfp = open(LOC_SLOW"log",O_RDWR | O_CREAT | O_TRUNC | O_APPEND);
	if(slowfp < 0){
		close(slowfp);
		printf("fp2 open error\n");
	}
	ValueLog * log = (ValueLog *) malloc(sizeof(ValueLog));
	FastMem *fast = (FastMem *)malloc(sizeof(FastMem));
	SlowMem *slow = (SlowMem *)malloc(sizeof(SlowMem));

	//fast
	log->fast = fast;
	log->fast->fp = fastfp;
	log->fast->head = head;
	log->fast->tail = tail;
	log->fast->curstart = head;
	log->fast->utili = 0;

	ftruncate(fastfp,FAST_MAX_PAGE);

	//slow
	log->slow = slow;
	log->slow->fp=slowfp;
	log->slow->head=head;
	log->slow->curhead = head;
	log->slow->cursize =1;

	ftruncate(slowfp,log->slow->cursize * MAX_PAGE);

	pthread_mutex_init(&log->fast_lock,NULL);
	pthread_cond_init(&log->cond ,NULL);// PTHREAD_COND_INITIALIZER;
	return log;
}
void SlowPut(ValueLog *log, int *loc, const char *key, uint64_t key_len, uint64_t value){

	SaveLog *save = (SaveLog *)malloc(sizeof(SaveLog));

	save->key_len = key_len;
	strncpy(save->key,key,key_len);
	save->value = value;

	SaveLog *map = mmap(0,MAPPING_LOG_SIZE,PROT_WRITE,MAP_SHARED,log->slow->fp,INDEX(log->slow->head) * MAPPING_LOG_SIZE);
	if(map == NULL){
		printf("error mapping file3\n");
		return;
	}

	memcpy(map+OFFSET(log->slow->head), save, sizeof(SaveLog));

	if(msync(map,MAPPING_LOG_SIZE,MS_SYNC) == -1){
		printf("could not sync the file to disk\n");
	}
	if(munmap(map,MAPPING_LOG_SIZE) == -1){
		printf("colud not unmap\n");
		return;
	}
	*loc = log->slow->head + (MAX_LOG_SIZE + 1);

	log->slow->head += sizeof(SaveLog);
	if(((log->slow->head - (INDEX(log->slow->head) * MAPPING_LOG_SIZE)) % MAX_LOG_MAPPING == 0))
		log->slow->head += (MAPPING_LOG_SIZE - MAX_LOG_MAPPING);

	struct stat sb;
	if(fstat(log->slow->fp, &sb) == -1){
		printf("fstat error\n");
		return;
	}

	if(sb.st_size >= log->slow->cursize * MAX_PAGE)
		ftruncate(log->slow->fp,(++log->slow->cursize) * MAX_PAGE);


	free(save);	
}

int ValuePut(LSMtree *lsm,ValueLog *log, int *loc, const char * key, uint64_t key_len, uint64_t value,int flag){

	SaveLog *save = (SaveLog *)malloc(sizeof(SaveLog));

	save->key_len = key_len;
	strncpy(save->key,key,key_len);
	save->value = value;

	pthread_mutex_lock(&log->fast_lock);
	if(flag==1&& log->fast->utili >= UTILIZATION){
	   // pthread_rwlock_unlock(&lsm->merge_lock);
		pthread_cond_wait(&log->cond,&log->fast_lock);
		//pthread_rwlock_rdlock(&lsm->merge_lock);
	}

/*	int nextloc = log->fast->head + sizeof(SaveLog);
	if(((nextloc - (INDEX(nextloc) * MAPPING_LOG_SIZE)) % MAX_LOG_MAPPING == 0))
		nextloc += (MAPPING_LOG_SIZE - MAX_LOG_MAPPING);

	nextloc = nextloc % FAST_MAX_PAGE;

	if(flag && nextloc == log->fast->tail){
		if(flag)
			pthread_rwlock_unlock(&lsm->merge_lock);
		pthread_cond_wait(&log->cond,&log->fast_lock);
	//	free(save);
	//	pthread_rwlock_unlock(&log->fast_lock);
	//	return -1;
		if(flag)
			pthread_rwlock_rdlock(&lsm->merge_lock);
	}*/

	SaveLog *map = mmap(NULL,MAPPING_LOG_SIZE,PROT_WRITE|PROT_READ,MAP_SHARED,log->fast->fp,INDEX(log->fast->head) * MAPPING_LOG_SIZE);

	if(map == MAP_FAILED){
		printf("error mapping file4\n");
		pthread_mutex_unlock(&log->fast_lock);
		return -1;
	}

	memcpy(map + OFFSET(log->fast->head), save, sizeof(SaveLog));

	if(msync(map,MAPPING_LOG_SIZE,MS_SYNC) == -1){
		printf("could not sync the file to disk\n");
	}

	if(munmap(map,MAPPING_LOG_SIZE) == -1){
		printf("colud not unmap\n");
		pthread_mutex_unlock(&log->fast_lock);
		return -1;
	}

	log->fast->utili += sizeof(SaveLog);
	*loc = log->fast->head;
	
	int	nextloc = log->fast->head + sizeof(SaveLog);
	if(((nextloc - (INDEX(nextloc) * MAPPING_LOG_SIZE)) % MAX_LOG_MAPPING == 0))
		nextloc += (MAPPING_LOG_SIZE - MAX_LOG_MAPPING);

	nextloc = nextloc % FAST_MAX_PAGE;
	
	log->fast->head = nextloc;
	pthread_mutex_unlock(&log->fast_lock);
	
	free(save);	
	return 1;
}

uint64_t ValueGet(ValueLog *log,int loc){

	if(loc == -1){
		printf("location is -1\n");
		return -1;
	}
	uint64_t value = -1;
	int fp = -1;
	bool slow = false;

	if(loc >= (MAX_LOG_SIZE + 1)){
		loc -= (MAX_LOG_SIZE + 1);
		fp = log->slow->fp;
		slow = true;
		printf("slow\n");
	}
	else{
		pthread_mutex_lock(&log->fast_lock);
		fp = log->fast->fp;
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

	SaveLog *map = mmap(0,MAPPING_LOG_SIZE , PROT_READ | PROT_WRITE, MAP_SHARED, fp,INDEX(loc) * MAPPING_LOG_SIZE);

	if(map == MAP_FAILED){
		if(!slow)
			pthread_mutex_unlock(&log->fast_lock);
		return value;
	}

	memcpy(save,map + OFFSET(loc),sizeof(SaveLog));

	value = save->value;

	free(save);

	if(munmap(map,MAPPING_LOG_SIZE) == -1){
		printf("colud not unmap\n");
	}
	if(!slow)
		pthread_mutex_unlock(&log->fast_lock);

	return value;
}

void GC(LSMtree *lsm,ValueLog *log){
	int end = log->fast->head;	
	int size = 0;
	
	int tail = log->fast->tail;

	printf("ready\n");
	while(tail != end){
		SaveLog * save = (SaveLog *)malloc(sizeof(SaveLog));

		SaveLog *map = mmap(0,MAPPING_LOG_SIZE,PROT_READ | PROT_WRITE, MAP_SHARED, log->fast->fp,INDEX(tail)*MAPPING_LOG_SIZE);
		
		if(map == MAP_FAILED){
			printf("error mapping file4\n");
			return;
		}

		memcpy(save,map + OFFSET(tail),sizeof(SaveLog));
		
		if(munmap(map,MAPPING_LOG_SIZE) == -1){
			printf("colud not unmap\n");
		}

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
		if(tail != 0 && ((tail - (INDEX(tail) * MAPPING_LOG_SIZE)) % MAX_LOG_MAPPING == 0))
			tail += (MAPPING_LOG_SIZE - MAX_LOG_MAPPING);

		tail = tail % FAST_MAX_PAGE;
		log->fast->tail = tail;
		
		pthread_cond_signal(&log->cond);
		pthread_rwlock_unlock(&lsm->merge_lock);

	}
	pthread_cond_broadcast(&log->cond);
	printf("done\n");
}
void ClearLog(ValueLog *log){
	close(log->fast->fp);
	close(log->slow->fp);
	free(log);
}
