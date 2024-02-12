#include "global.h"

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
	
	fseek(log->fp, (int) log->head, SEEK_SET);
	
	fwrite(&key_len, sizeof(uint64_t),1, log->fp);
	
	fwrite(key, sizeof(char),key_len, log->fp);
	
	fwrite(&value, sizeof(uint64_t),1,log-> fp);

	*loc = log->head;

	log->head += sizeof(uint64_t) + key_len + sizeof(uint64_t);
	
}

uint64_t ValueGet(ValueLog *log,int loc){

	fseek(log->fp, (int) loc, SEEK_SET);

	uint64_t key_len;
	fread(&key_len,sizeof(uint64_t),1,log->fp);

	fseek(log->fp, (int)key_len, SEEK_CUR);
	
	uint64_t value;
	fread(&value,sizeof(uint64_t),1,log->fp);

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
void ClearLog(ValueLog *log){
	fclose(log->fp);
	free(log);
}
