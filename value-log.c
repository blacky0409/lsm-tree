#include "global.h"

#define READ_LOG_SIZE 10

ValueLog *CreateLog(int head, int tail){

	char filename[25] = "data/valuelog/log";
	FILE *fp = fopen(filename, "w+");
	if(fp == NULL){
		printf("fp error : %s\n",strerror(errno));
	}
	ValueLog * log = (ValueLog *) malloc(sizeof(ValueLog));
	log->fp = fp;
	log->head = head;
	log->tail = tail;

	return log;
}

void ValuePut(ValueLog *log, int *loc, const char * key, uint64_t key_len, uint64_t value){

	if(log->tail == (log->head + sizeof(SaveLog)) || (log->tail == 0 && (log->head + sizeof(SaveLog) > 70000))){
			printf("full..\n");
			exit(-1);
	}
	SaveLog *save = (SaveLog *)malloc(sizeof(SaveLog));
	
	save->key_len = key_len;
	strncpy(save->key,key,key_len);
	save->value = value;

	fseek(log->fp, (int) log->head, SEEK_SET);
	
	fwrite(save, sizeof(SaveLog),1, log->fp);
	
	*loc = log->head;

	log->head += sizeof(SaveLog);

	if(log->head > 70000)
		log->head = 0;
	free(save);	
}

uint64_t ValueGet(ValueLog *log,int loc){
	
	uint64_t value = -1;
	if((log->tail < log->head && (loc >= log->tail)) || 
			((log->tail > log->head) && (log->tail < 70000 || loc < log->head))){
		SaveLog *save = (SaveLog *)malloc(sizeof(SaveLog));

		fseek(log->fp, (int) loc, SEEK_SET);

		fread(save,sizeof(SaveLog),1,log->fp);

		value = save->value;

		free(save);

	}
	
	return value;
}
int ValueLog_sync(ValueLog *log) {
    int res = fflush(log->fp);
    if (res == EOF) {
        perror("fflush");
        return -1;
    }

    res = fsync(fileno(log->fp));
    if (res == -1) {
        perror("fsync");
        return -1;
    }
    return 0;
}

void GC(LSMtree *lsm,ValueLog *log){
	int end = log->head;	
	int size = 0;
	while(log->tail != end){
		SaveLog * save;
		save = (SaveLog *)malloc(sizeof(SaveLog) * READ_LOG_SIZE);
		SaveLog * save_keep = save;
		fseek(log->fp,log->tail,SEEK_SET);
		fread(save,sizeof(SaveLog),READ_LOG_SIZE,log->fp);
		size = 0;
		printf("hi\n");
		for(; size < READ_LOG_SIZE; size++){
				char str_key[STRING_SIZE];
				strncpy(str_key,save[size].key,save[size].key_len);
				SaveArray * dest = Get_array(lsm,str_key);
				if(dest != NULL){
					int loc;
					ValuePut(log,&loc,str_key,save[size].key_len,save[size].value);
					dest->array[dest->index].value = loc;
					
					printf("%s\n",dest->filename);
					if(strcmp(dest->filename,"")!=0){
						FILE *fp = fopen(dest->filename, "wt");
						if(fp == NULL)
							printf("what\n");
						fwrite(dest->array, sizeof(Node), dest->size, fp);
						fclose(fp);
					}
				}
				else
					printf("Can't find key\n");
				dest = NULL;
		}
		log->tail += (sizeof(SaveLog) * size);
		if(log->tail > 70000)
			log->tail = 0;
		free(save_keep);
	}

	if(log->tail > log->head){
		log->tail = log->head;
	}
}
void ClearLog(ValueLog *log){
	fclose(log->fp);
	free(log);
}
