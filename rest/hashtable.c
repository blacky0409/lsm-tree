#include "global.h"

HashTable *CreateHashTable(int size){
	HashTable *table = (HashTable *) malloc(sizeof(HashTable));
	if(table == NULL){
		printf("There is not enough memory for a hash table.");
		return NULL;
	}
	table->count = 0;
	table->array = (char *) malloc(size * sizeof(char));
	if(table->array == NULL){
		printf("There is not enough memory for an array in the hash table.");
	}
	return table;
}

void AddToTable(HashTable *table, char * key){
	strcpy(table->array[table->count] , key);
	table->count += 1;
	return;
}

bool CheckTable(HashTable *table, char * key){
	int i;
	for(i = 0; i < table->count; i++){
		if(strcmp(table->array[i] , key) == 0){
			return true;
		}
	}
	return false;
}

void ClearTable(HashTable *table){
	free(table->array);
	free(table);
}

