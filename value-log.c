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
	
	int res = fseek(log->fp, (int) log->head, SEEK_SET);
	
	int b_writed = fwrite(&key_len, sizeof(uint64_t),1, log->fp);
	
	b_writed = fwrite(key, sizeof(char),key_len, log->fp);
	
	b_writed = fwrite(&value, sizeof(uint64_t),1,log-> fp);

	*loc = log->head;

	log->head += sizeof(uint64_t) + key_len + sizeof(uint64_t);
	
}

uint64_t ValueGet(ValueLog *log,int loc){

	int res = fseek(log->fp, (int) loc, SEEK_SET);

	uint64_t key_len;
	int b_read = fread(&key_len,sizeof(uint64_t),1,log->fp);

	res = fseek(log->fp, (int)key_len, SEEK_CUR);
	
	uint64_t value;
	b_read = fread(&value,sizeof(uint64_t),1,log->fp);

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
