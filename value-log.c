#include "global.h"

#define READ_LOG_SIZE 10
#define LOC_SLOW "SlowMemory/"
#define TO_SLOW 5

ValueLog *CreateLog(int head, int tail){
	FILE *fastfp1 = fopen(LOC_FAST"log", "w+");
	if(fastfp1 == NULL){
		printf("fp error : %s\n",strerror(errno));
	}
	FILE *fastfp2 = fopen(LOC_FAST"log2","w+");
	if(fastfp2 == NULL){
		printf("fp2 error : %s\n",strerror(errno));
	}
	FILE *slowfp = fopen(LOC_SLOW"log","w+");
	if(slowfp == NULL){
		printf("fp2 error : %s\n",strerror(errno));
	}
	ValueLog * log = (ValueLog *) malloc(sizeof(ValueLog));
	FastMem *fast = (FastMem *)malloc(sizeof(FastMem));
	SlowMem *slow = (SlowMem *)malloc(sizeof(SlowMem));

	//fast
	log->fast = fast;
	log->fast->fp1 = fastfp1;
	log->fast->fp2 = fastfp2; 
	log->fast->head = head;
	log->fast->tail = tail;
	log->fast->curhead = fastfp1;
	log->fast->curtail = fastfp1;

	//slow
	log->slow = slow;
	log->slow->fp=slowfp;
	log->slow->head=head;
	return log;
}
void SlowPut(ValueLog *log, int *loc, const char *key, uint64_t key_len, uint64_t value){

	SaveLog *save = (SaveLog *)malloc(sizeof(SaveLog));

	save->key_len = key_len;
	strncpy(save->key,key,key_len);
	save->value = value;

	fseek(log->slow->fp, (int) log->slow->head, SEEK_SET);

	fwrite(save, sizeof(SaveLog),1, log->slow->fp);

	ValueLog_sync(log->slow->fp);

	*loc = log->slow->head;

	*loc += (2*MAX_LOG_SIZE + 2);


	log->slow->head += sizeof(SaveLog);

	free(save);	
}

void ValuePut(ValueLog *log, int *loc, const char * key, uint64_t key_len, uint64_t value){

	if(log->fast->curhead != log->fast->curtail){
		if(log->fast->head > MAX_LOG_SIZE - sizeof(SaveLog)){
			printf("Log file is full\n");
			exit(-1);
		}
	}
	else if((log->fast->tail > log->fast->head  && log->fast->curtail == log->fast->curhead)){
		printf("Log file is full2\n");
		exit(-1);
	}
	SaveLog *save = (SaveLog *)malloc(sizeof(SaveLog));

	save->key_len = key_len;
	strncpy(save->key,key,key_len);
	save->value = value;

	fseek(log->fast->curhead, (int) log->fast->head, SEEK_SET);

	fwrite(save, sizeof(SaveLog),1, log->fast->curhead);

	ValueLog_sync(log->fast->curhead);

	*loc = log->fast->head;
	if(log->fast->curhead == log->fast->fp2)
		*loc += (MAX_LOG_SIZE + 1);


	log->fast->head += sizeof(SaveLog);

	if(log->fast->head >= MAX_LOG_SIZE){
		log->fast->head = 0;
		if(log->fast->curhead == log->fast->curtail){
			if(log->fast->curhead == log->fast->fp1){
				fclose(log->fast->fp2);
				FILE *fp2 = fopen(LOC_FAST"log2","w+");
				if(fp2 == NULL){
					printf("fp2 error : %s\n",strerror(errno));
				}
				log->fast->fp2 = fp2;
				log->fast->curhead = log->fast->fp2;
			}
			else if(log->fast->curhead == log->fast->fp2){
				fclose(log->fast->fp1);
				FILE *fp1 = fopen(LOC_FAST"log","w+");
				if(fp1 == NULL){
					printf("fp1 error : %s\n",strerror(errno));
				}
				log->fast->fp1 = fp1;
				log->fast->curhead = log->fast->fp1;

			}
		}
	}
	free(save);	
}

uint64_t ValueGet(ValueLog *log,int loc){

	uint64_t value = -1;
	FILE * fp;
	bool slow = false;


	if(loc >= (2*MAX_LOG_SIZE) + 2){
		loc -= ((2*MAX_LOG_SIZE) + 2);
		fp = log->slow->fp;
		slow = true;
	}
	else if(loc >= MAX_LOG_SIZE + 1){
		loc -= (MAX_LOG_SIZE + 1);
		fp = log->fast->fp2;
	}
	else{
		fp = log->fast->fp1;
	}

	ValueLog_sync(fp);

	if(!slow){
		if(fp == log->fast->curhead && fp == log->fast->curtail){
			if(log->fast->tail > loc || log->fast->head < loc)
				return value;
		}
		else if(fp == log->fast->curhead){
			if(log->fast->head <loc)
				return value;
		}
		else{
			if(log->fast->tail > loc)
				return value;
		}
	}

	SaveLog *save = (SaveLog *)malloc(sizeof(SaveLog));

	fseek(fp, (int) loc, SEEK_SET);

	fread(save,sizeof(SaveLog),1,fp);

	value = save->value;

	free(save);

	return value;
}
int ValueLog_sync(FILE *fp) {
	int res = fflush(fp);
	if (res == EOF) {
		perror("fflush");
		return -1;
	}

	res = fsync(fileno(fp));
	if (res == -1) {
		perror("fsync");
		return -1;
	}
	return 0;
}

void GC(LSMtree *lsm,ValueLog *log){
	int end = log->fast->head;	
	FILE * endfile = log->fast->curhead;
	int size = 0;
	bool finish = false;
	while(true){
		if(log->fast->curtail == endfile && ( log->fast->tail + sizeof(SaveLog) >= end))
			finish = true;
		SaveLog * save;
		save = (SaveLog *)malloc(sizeof(SaveLog) * READ_LOG_SIZE);
		SaveLog * save_keep = save;
		fseek(log->fast->curtail,log->fast->tail,SEEK_SET);
		fread(save,sizeof(SaveLog),READ_LOG_SIZE,log->fast->curtail);
		size = 0;
		for(; size < READ_LOG_SIZE && !finish; size++){
			if(log->fast->tail + sizeof(SaveLog) >= MAX_LOG_SIZE)
				finish = true;
			if(log->fast->curtail == endfile && log->fast->tail + sizeof(SaveLog) >= end)
				finish = true;

			char str_key[STRING_SIZE];
			strncpy(str_key,save[size].key,save[size].key_len);
			SaveArray * dest = Get_array(lsm,str_key);
			if(dest != NULL){
				int loc = 0;
				if(dest->number < TO_SLOW)
					ValuePut(log,&loc,str_key,save[size].key_len,save[size].value);
				else
					SlowPut(log,&loc,str_key,save[size].key_len,save[size].value);

				dest->array[dest->index].value = loc;

				if(strcmp(dest->filename,"")!=0){
					FILE *fp = fopen(dest->filename, "wt");
					if(fp == NULL)
						printf("fp error\n");
					fwrite(dest->array, sizeof(Node), dest->size, fp);
					fclose(fp);

				}
			}
			else
				printf("Can't find key\n");
			dest = NULL;
			log->fast->tail += sizeof(SaveLog);
		}
		if(log->fast->tail >= MAX_LOG_SIZE){
			log->fast->tail = 0;
			if(log->fast->curtail == log->fast->fp1)
				log->fast->curtail = log->fast->fp2;
			else
				log->fast->curtail = log->fast->fp1;
		}
		free(save_keep);
		if(finish)
			break;
	}
}
void ClearLog(ValueLog *log){
	fclose(log->fast->fp1);
	fclose(log->fast->fp2);
	fclose(log->slow->fp);
	free(log);
}
