#include "global.h"

#define READ_LOG_SIZE 10

ValueLog *CreateLog(int head, int tail){

	char filename[25] = "data/valuelog/log";
	FILE *fp = fopen(filename, "w+");
	if(fp == NULL){
		printf("fp error : %s\n",strerror(errno));
	}
	char filename2[25] = "data/valuelog/log2";
	FILE *fp2 = fopen(filename2,"w+");
	if(fp == NULL){
		printf("fp2 error : %s\n",strerror(errno));
	}
	ValueLog * log = (ValueLog *) malloc(sizeof(ValueLog));
	log->fp1 = fp;
	log->fp2 = fp2; 
	log->head = head;
	log->tail = tail;
	log->curhead = fp;
	log->curtail = fp;

	return log;
}

void ValuePut(ValueLog *log, int *loc, const char * key, uint64_t key_len, uint64_t value){

	if(log->curhead != log->curtail){
			if(log->head > MAX_LOG_SIZE - sizeof(SaveLog)){
				printf("Log file is full\n");
				exit(-1);
			}
	}
	else if((log->tail > log->head  && log->curtail == log->curhead)){
		printf("Log file is full2\n");
		exit(-1);
	}
	SaveLog *save = (SaveLog *)malloc(sizeof(SaveLog));
	
	save->key_len = key_len;
	strncpy(save->key,key,key_len);
	save->value = value;

	fseek(log->curhead, (int) log->head, SEEK_SET);
	
	fwrite(save, sizeof(SaveLog),1, log->curhead);
	
	ValueLog_sync(log->curhead);
	
	*loc = log->head;
	if(log->curhead == log->fp2)
		*loc += (MAX_LOG_SIZE + 1);


	log->head += sizeof(SaveLog);

	if(log->head >= MAX_LOG_SIZE){
		log->head = 0;
		if(log->curhead == log->curtail){
			if(log->curhead == log->fp1){
				fclose(log->fp2);
				char filename2[25] = "data/valuelog/log2";
				FILE *fp2 = fopen(filename2,"w+");
				if(fp2 == NULL){
					printf("fp2 error : %s\n",strerror(errno));
				}
				log->fp2 = fp2;
				log->curhead = log->fp2;
			}
			else if(log->curhead == log->fp2){
				fclose(log->fp1);
				char filename[25] = "data/valuelog/log";
				FILE *fp1 = fopen(filename,"w+");
				if(fp1 == NULL){
					printf("fp1 error : %s\n",strerror(errno));
				}
				log->fp1 = fp1;
				log->curhead = log->fp1;
		
			}
		}
	}
	free(save);	
}

uint64_t ValueGet(ValueLog *log,int loc){
	
	uint64_t value = -1;
	FILE * fp;

	if(loc >= MAX_LOG_SIZE + 1){
		loc -= (MAX_LOG_SIZE + 1);
		fp = log->fp2;
	}
	else{
		fp = log->fp1;
	}

	if(fp == log->curhead && fp == log->curtail){
		if(log->tail > loc || log->head < loc)
			return value;
	}
	else if(fp == log->curhead){
		if(log->head <loc)
			return value;
	}
	else{
		if(log->tail > loc)
			return value;
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
	int end = log->head;	
	FILE * endfile = log->curhead;
	int size = 0;
	bool finish = false;
	while(true){
		if(log->curtail == endfile && ( log->tail + sizeof(SaveLog) >= end))
			finish = true;
		SaveLog * save;
		save = (SaveLog *)malloc(sizeof(SaveLog) * READ_LOG_SIZE);
		SaveLog * save_keep = save;
		fseek(log->curtail,log->tail,SEEK_SET);
		fread(save,sizeof(SaveLog),READ_LOG_SIZE,log->curtail);
		size = 0;
		for(; size < READ_LOG_SIZE && !finish; size++){
				if(log->tail + sizeof(SaveLog) >= MAX_LOG_SIZE)
					finish = true;
				if(log->curtail == endfile && log->tail + sizeof(SaveLog) >= end)
					finish = true;
			
				char str_key[STRING_SIZE];
				strncpy(str_key,save[size].key,save[size].key_len);
				SaveArray * dest = Get_array(lsm,str_key);
				if(dest != NULL){
					int loc = 0;
					ValuePut(log,&loc,str_key,save[size].key_len,save[size].value);
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
				log->tail += sizeof(SaveLog);
		}
		if(log->tail >= MAX_LOG_SIZE){
			log->tail = 0;
			if(log->curtail == log->fp1)
				log->curtail = log->fp2;
			else
				log->curtail = log->fp1;
		}
		free(save_keep);
		if(finish)
			break;
	}
}
void ClearLog(ValueLog *log){
	fclose(log->fp1);
	fclose(log->fp2);
	free(log);
}
