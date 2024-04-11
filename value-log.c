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
	SaveLog *map = (SaveLog *)mmap(0,MAPPING_LOG_SIZE,PROT_WRITE|PROT_READ,MAP_SHARED,log->fast->fp,0);

	if(map == MAP_FAILED){
		printf("error mapping file1\n");
		return NULL;
	}

	log->fast->map = map;

	//slow
	log->slow = slow;
	log->slow->fp=slowfp;
	log->slow->head=head;
	log->slow->curhead = head;
	log->slow->cursize =1;

	ftruncate(slowfp,log->slow->cursize * MAX_PAGE);
	SaveLog *map2 = mmap(NULL,MAPPING_LOG_SIZE,PROT_WRITE|PROT_READ,MAP_SHARED,log->slow->fp,log->slow->head);

	if(map == NULL){
		printf("error mapping file2\n");
		return NULL;
	}
	log->slow->map = map2;

	pthread_rwlock_init(&(log->fastlock),NULL);
	pthread_rwlock_init(&(log->slowlock),NULL);

	return log;
}
void SlowPut(ValueLog *log, int *loc, const char *key, uint64_t key_len, uint64_t value){
	
	SaveLog *save = (SaveLog *)malloc(sizeof(SaveLog));

	save->key_len = key_len;
	strncpy(save->key,key,key_len);
	save->value = value;

	pthread_rwlock_wrlock(&(log->slowlock));
	if(INDEX(log->slow->head) != INDEX(log->slow->curhead)){
		log->slow->cursize++;
		ftruncate(log->slow->fp,MAX_PAGE * log->slow->cursize);

		if(munmap(log->slow->map,MAPPING_LOG_SIZE) == -1){
			printf("colud not unmap\n");
		}

		log->slow->curhead = INDEX(log->slow->head) * MAPPING_LOG_SIZE;

		SaveLog *map = mmap(0,MAPPING_LOG_SIZE,PROT_WRITE,MAP_SHARED,log->slow->fp,log->slow->curhead);

		if(map == NULL){
			printf("error mapping file3\n");
			pthread_rwlock_unlock(&(log->slowlock));
			return;
		}

		log->slow->map = map;

	}

	memcpy(log->slow->map+OFFSET(log->slow->head), save, sizeof(SaveLog));

	if(msync(log->slow->map,MAPPING_LOG_SIZE,MS_SYNC) == -1){
		printf("could not sync the file to disk\n");
	}

	*loc = log->slow->head + (MAX_LOG_SIZE + 1);

	log->slow->head += sizeof(SaveLog);
	if(((log->slow->head - (INDEX(log->slow->head) * MAPPING_LOG_SIZE)) % MAX_LOG_MAPPING == 0))
		log->slow->head += (MAPPING_LOG_SIZE - MAX_LOG_MAPPING);
	pthread_rwlock_unlock(&(log->slowlock));

	free(save);	
}

int ValuePut(LSMtree *lsm,ValueLog *log, int *loc, const char * key, uint64_t key_len, uint64_t value){
	
	pthread_rwlock_wrlock(&(log->fastlock));
	int nextloc = log->fast->head + sizeof(SaveLog);
	if(((nextloc - (INDEX(nextloc) * MAPPING_LOG_SIZE)) % MAX_LOG_MAPPING == 0))
		nextloc += (MAPPING_LOG_SIZE - MAX_LOG_MAPPING);

	nextloc = nextloc % FAST_MAX_PAGE;

	if(nextloc == log->fast->tail){
		printf("log is full\n");
		return -1;
	}

	SaveLog *save = (SaveLog *)malloc(sizeof(SaveLog));

	save->key_len = key_len;
	strncpy(save->key,key,key_len);
	save->value = value;

	if(INDEX(log->fast->head) != INDEX(log->fast->curstart)){
		if(munmap(log->fast->map,MAPPING_LOG_SIZE) == -1){
			printf("colud not unmap\n");
			return -1;
		}

		log->fast->curstart = INDEX(log->fast->head) * MAPPING_LOG_SIZE;

		SaveLog *map = mmap(NULL,MAPPING_LOG_SIZE,PROT_WRITE|PROT_READ,MAP_SHARED,log->fast->fp,log->fast->curstart);

		if(map == MAP_FAILED){
			printf("error mapping file4\n");
			exit(-1);
			//return -1;
		}

		log->fast->map = map;
	}

	memcpy(log->fast->map + OFFSET(log->fast->head), save, sizeof(SaveLog));

	if(msync(log->fast->map,MAPPING_LOG_SIZE,MS_SYNC) == -1){
		printf("could not sync the file to disk\n");
	}

	log->fast->utili += sizeof(SaveLog);
	*loc = log->fast->head;
	
	log->fast->head = nextloc;

	pthread_rwlock_unlock(&(log->fastlock));
	free(save);	
	return 1;
}

uint64_t ValueGet(ValueLog *log,int loc){
	
	if(loc == -1){
		return -1;
	}
	uint64_t value = -1;
	int fp = -1;
	bool slow = false;
	int head;

	SaveLog * maped;

	if(loc >= (MAX_LOG_SIZE + 1)){
		loc -= (MAX_LOG_SIZE + 1);
		fp = log->slow->fp;
		slow = true;
		head = INDEX(log->slow->curhead);
		maped = log->slow->map;
		printf("slow\n");
	}
	else{
		fp = log->fast->fp;
		head = INDEX(log->fast->curstart);
		maped = log->fast->map;
		printf("fast\n");
	}
	if(!slow){
		if(log->fast->head > log->fast->tail && (log->fast->head <= loc || log->fast->tail > loc)){
			printf("fault 1\n");
			exit(-1);
		}
		else if(log->fast->head < log->fast->tail && (log->fast->tail > loc && log->fast->head <=loc)){
			printf("fault 2\n");
			exit(-1);
		}
	
	}
	else{
		if(loc >= log->slow->head){
			printf("fault3\n");
			return value;
		}
	}
	SaveLog *save = (SaveLog *)malloc(sizeof(SaveLog));

	int curhead = INDEX(loc);
	
	if(!slow)
		pthread_rwlock_wrlock(&(log->fastlock));
	else		
		pthread_rwlock_wrlock(&(log->slowlock));
	if(curhead != head){
		if(!slow){
			log->fast->curstart = curhead * MAPPING_LOG_SIZE;
		}
		else{
			log->slow->curhead = curhead * MAPPING_LOG_SIZE;
		}

		if(munmap(maped,MAPPING_LOG_SIZE) == -1){
			printf("colud not unmap\n");
		}

		SaveLog *map = mmap(0,MAPPING_LOG_SIZE , PROT_READ | PROT_WRITE, MAP_SHARED, fp,curhead * MAPPING_LOG_SIZE);

		if(map == MAP_FAILED){
			if(!slow)
				pthread_rwlock_unlock(&(log->fastlock));
			else
				pthread_rwlock_unlock(&(log->slowlock));
			return value;
		}
		if(!slow){
			log->fast->map = map;
		}	
		else{
			log->slow->map = map;
		}
		maped = map;
	}
	memcpy(save,maped + OFFSET(loc),sizeof(SaveLog));

	value = save->value;

	free(save);
	
	if(!slow)
		pthread_rwlock_unlock(&(log->fastlock));
	else	
		pthread_rwlock_unlock(&(log->slowlock));
	return value;
}

void GC(LSMtree *lsm,ValueLog *log){
	int end = log->fast->head;	
	int size = 0;


	pthread_rwlock_wrlock(&lsm->GC_lock);
	printf("ready\n");
	while(log->fast->tail != end){
		SaveLog * save = (SaveLog *)malloc(sizeof(SaveLog));

		if(INDEX(log->fast->curstart) != INDEX(log->fast->tail)){
			if(munmap(log->fast->map,MAPPING_LOG_SIZE) == -1){
				printf("colud not unmap\n");
			}

			log->fast->curstart = INDEX(log->fast->tail) * MAPPING_LOG_SIZE;

			log->fast->map = mmap(0,MAPPING_LOG_SIZE,PROT_READ | PROT_WRITE, MAP_SHARED, log->fast->fp,log->fast->curstart);
		}

		memcpy(save,log->fast->map + OFFSET(log->fast->tail),sizeof(SaveLog));
		char str_key[STRING_SIZE];
		strncpy(str_key,save->key,save->key_len);
		SaveArray * dest = Get_array(lsm,str_key);
		
		if(dest != NULL){
			int loc = 0;
			if(dest->number <= TO_SLOW){
				ValuePut(lsm,log,&loc,str_key,save[size].key_len,save[size].value);
			}
			else{
				SlowPut(log,&loc,str_key,save[size].key_len,save[size].value);
			}
			log->fast->utili -= sizeof(SaveLog);
			dest->array[dest->index].value = loc;

			if(strcmp(dest->filename,"")!=0){
				FILE *fp = fopen(dest->filename, "wt");
				if(fp == NULL)
					printf("fp error\n");
				fwrite(dest->array, sizeof(Node), dest->size, fp);
				fclose(fp);
			}
			else{
				memcpy(lsm->buffer->array,dest->array, dest->size * sizeof(Node));
			}
		}
		else
			printf("Can't find key\n");
		dest = NULL;
		log->fast->tail += sizeof(SaveLog);
		if(log->fast->tail != 0 && ((log->fast->tail - (INDEX(log->fast->tail) * MAPPING_LOG_SIZE)) % MAX_LOG_MAPPING == 0))
			log->fast->tail += (MAPPING_LOG_SIZE - MAX_LOG_MAPPING);
		
		log->fast->tail = log->fast->tail % FAST_MAX_PAGE;
	}
	printf("done\n");
	pthread_rwlock_unlock(&lsm->GC_lock);
}
void ClearLog(ValueLog *log){
	close(log->fast->fp);
	close(log->slow->fp);
	free(log);
}
