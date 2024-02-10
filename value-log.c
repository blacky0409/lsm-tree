#include "global.h"

ValueLog *CreateLog(size_t head, size_t tail){

	FILE *fp = fopen("/data/valuelog/log", "r+");
	
	ValueLog * log = (ValueLog *) malloc(sizeof(ValueLog));

	log->fp = fp;
	log->head = head;
	log->tail = tail;

	return log;
}

void ValuePut(ValueLog *log, size_t *loc, const char * key, uint64_t key_len, int value){
	int res = fseek(log->file, (long) log->head, SEEK_SET);

	int b_writed = fwrite(&key_len, sizeof(uint64_t),1, log->file);

	b_writed = fwrite(key, sizeof(char),key_len, log->file);
	
	b_writed = fwrite(&value, sizeof(int),1, log->file);

	*loc = log->head;

	log->head += sizeof(uint64_t) + key_len + sizeof(int);

	
}

int ValueGet(ValueLog *log,size_t loc){
	int res = fseek(log->file, (long) loc, SEEK_SET);

	uint64_t key_len;
	int b_read = fread(&key_len,sizeof(uint64_t),1,log->file);

	res = fseek(log->file, (long)key_len, SEEK_CUR);
	
	int value;
	b_read = fread(&value,sizeof(int),1,log->file);

	return value;
}

void ClearLog(ValueLog *log){
	int res = fclose(log->file);

	free(log);
}
