#include "global.h"
#define INT_MAX 2147483647

int cmpfunc(const void *a, const void *b){
	return strcmp(((Node *)a)->key,((Node *)b)->key);
}

int Delete_flag(Node * sort, int size){

	int index = 0;

	for(int i = 0 ; i < size; i++){
		if(sort[i].flag){
			sort[index] = sort[i];
			index ++;
		}
	}

	return index;
}

void Merge(LSMtree * lsm,LevelNode *Current, int origin, int levelsize,
		int runcount, int runsize, Node *sortedrun, double targetfpr){
	char * start = (char *) malloc(sizeof(char) * STRING_SIZE);
	char * end = (char *) malloc(sizeof(char) * STRING_SIZE);
	printf("merge start\n");

	if(Current->next == NULL){ //no level
		pthread_mutex_lock(&lsm->make_level_lock);
		Current->next = (LevelNode *) malloc(sizeof(LevelNode));
		Current->next->level = CreateLevel(levelsize, targetfpr);
		pthread_mutex_unlock(&lsm->make_level_lock);
	
		strcpy(start,sortedrun[0].key);
		strcpy(end, sortedrun[runcount - 1].key);

		pthread_rwlock_wrlock(&Current->next->level->level_lock);
		runcount = Delete_flag(sortedrun, runcount);
		char filename[FILE_NAME];
		sprintf(filename, LOC_FAST"data/L%dN%d", (origin+1), Current->next->level->count);
		FILE *fp = fopen(filename, "wt");
		if(fp == NULL){
			fprintf(stderr, "Couldn't open %s: %s\n", filename, strerror(errno));	
		}
		fwrite(sortedrun, sizeof(Node), runcount, fp);
		fclose(fp);
		InsertRun(lsm,Current->next->level, runcount, runsize, start, end);
		Current->next->number = origin + 1;
		Current->next->next = NULL;
		pthread_rwlock_unlock(&Current->next->level->level_lock);

	}
	else{ //exist level
		int i;
		int j = 0;
		int min = INT_MAX;
		int minpos = -1;
		Level *destlevel = Current->next->level;

		int *distance = (int *) malloc(destlevel->count * sizeof(int));
		int *overlap = (int *) malloc(destlevel->count * sizeof(int));

		strcpy(start,sortedrun[0].key); //most small
		strcpy(end,sortedrun[runcount - 1].key); //most large
		
		pthread_rwlock_wrlock(&destlevel->level_lock);
		for(i = 0; i < destlevel->count; i++){
			distance[i] = 0;
			if(strcmp(destlevel->array[i].start , end) > 0){
				for(int w = 0; w < STRING_SIZE - 1 ; w++){
					distance[i] += (destlevel->array[i].start[w] - end[w]) * (1 << (STRING_SIZE - w - 2));
				}
			}else if(strcmp(destlevel->array[i].end , start) < 0){
				for(int w = 0; w < STRING_SIZE - 1 ; w++){
					distance[i] += (start[w] - destlevel->array[i].end[w]) * (1 << (STRING_SIZE - w - 2));
				}
			}else{
				if(j == 0){
					overlap[j] = i;
					j += 1;
				}else{
					int k;
					int n;
					for(k = 0; k < j; k++){
						if(strcmp(destlevel->array[overlap[k]].start , destlevel->array[i].start) > 0){
							break;
						}
					}
					for(n = j; n > k; n--){
						overlap[n] = overlap[n-1];
					}
					overlap[k] = i;
					j += 1;
				}
			}
			if((destlevel->array[i].count + runcount) < destlevel->array[i].size){
				if(distance[i] < min){
					min = distance[i];
					minpos = i;
				}
			}
		}
		if(j == 0){
			//겹치는 key가 없음
			if(destlevel->count < destlevel->size){

				strcpy(start,sortedrun[0].key);
				strcpy(end,sortedrun[runcount - 1].key);

				runcount = Delete_flag(sortedrun, runcount);

				char filename[FILE_NAME];
				sprintf(filename, LOC_FAST"data/L%dN%d", (origin+1), destlevel->count);
				FILE *fp = fopen(filename, "wt");
				fwrite(sortedrun, sizeof(Node), runcount, fp);
				fclose(fp);

				InsertRun(lsm,destlevel, runcount, runsize, start, end);

			}else{
				if(minpos != -1){
					Run oldrun = destlevel->array[minpos];
					Node *newarray = (Node *) malloc((oldrun.count + runcount) * sizeof(Node));

					Node *oldbump = (Node *) malloc(oldrun.count * sizeof(Node));
					char name[FILE_NAME];
					sprintf(name, LOC_FAST"data/L%dN%d", Current->next->number, minpos);
					FILE *fp = fopen(name, "rt");
					fread(oldbump, sizeof(Node), oldrun.count, fp);
					fclose(fp);

					int a = 0;
					int b = 0;
					int c = 0;
					while((a < oldrun.count) && (b < runcount)){

						if(strcmp(oldbump[a].key , sortedrun[b].key) < 0){
							if(oldbump[a].flag){
								newarray[c] = oldbump[a];
								a += 1;
								c += 1;
							}
							else{
								a += 1;
							}
						}else if(strcmp(oldbump[a].key , sortedrun[b].key) > 0 ){
							if(sortedrun[b].flag){
								newarray[c] = sortedrun[b];
								b += 1;
								c += 1;
							}
							else
								b += 1;
						}else{
							if(sortedrun[b].flag){
								newarray[c] = sortedrun[b];
								a += 1;
								b += 1;
								c += 1;
							}
							else if(oldbump[a].flag){
								newarray[c] = oldbump[a];
								a += 1;
								b += 1;
								c += 1;
							}
							else{
								a += 1;
								b += 1;
							}
						}
					}
					while(a < oldrun.count){
						if(oldbump[a].flag){
							newarray[c] = oldbump[a];
							a += 1;
							c += 1;
						}
						else
							a += 1;
					}
					while (b < runcount){
						if(sortedrun[b].flag){
							newarray[c] = sortedrun[b];
							b += 1;
							c += 1;
						}
						else
							b += 1;

					}

					free(oldbump);

					sprintf(name, LOC_FAST"data/L%dN%d", Current->next->number, minpos);
					fp = fopen(name, "wt");
					fwrite(newarray, sizeof(Node), (c), fp);
					fclose(fp);

					oldrun.count = c;
					strcpy(oldrun.start , newarray[0].key);
					strcpy(oldrun.end , newarray[c - 1].key);
					destlevel->array[minpos] = oldrun;
					free(newarray);
				}else{ //기존 sstable에 넣을 공간이 없음
					Run pushtonext = PopRun(lsm,destlevel);
					Node *topush = (Node *) malloc(pushtonext.count * sizeof(Node));

					char name[FILE_NAME];
					sprintf(name, LOC_FAST"data/L%dN%d", Current->next->number, destlevel->count);
					FILE *fp = fopen(name, "rt");
					fread(topush, sizeof(Node), pushtonext.count, fp);
					fclose(fp);

					Merge(lsm,Current->next, (origin + 1), levelsize, 
							pushtonext.count, pushtonext.size * (levelsize + 1), topush, (targetfpr * (levelsize + 1)));


					Run oldrun = destlevel->array[destlevel->count -1];
					Node *newarray = (Node *) malloc((oldrun.count + runcount) * sizeof(Node));


					Node *oldbump = (Node *) malloc(oldrun.count * sizeof(Node));
					sprintf(name, LOC_FAST"data/L%dN%d", Current->next->number, (destlevel->count - 1));
					fp = fopen(name, "rt");
					fread(oldbump, sizeof(Node), oldrun.count, fp);
					fclose(fp);

					int a = 0;
					int b = 0;
					int c = 0;
					while((a < oldrun.count) && (b < runcount)){

						if(strcmp(oldbump[a].key , sortedrun[b].key) < 0){
							if(oldbump[a].flag){
								newarray[c] = oldbump[a];
								a += 1;
								c += 1;
							}
							else
								a += 1;
						}else if(strcmp(oldbump[a].key , sortedrun[b].key) > 0 ){
							if(sortedrun[b].flag){
								newarray[c] = sortedrun[b];
								b += 1;
								c += 1;
							}
							else
								b += 1;
						}else{
							if(sortedrun[b].flag){
								newarray[c] = sortedrun[b];
								a += 1;
								b += 1;
								c += 1;
							}
							else if(oldbump[a].flag){
								newarray[c] = oldbump[a];
								a += 1;
								b += 1;
								c += 1;
							}
							else{
								a += 1;
								b += 1 ;
							}
						}
					}
					while(a < oldrun.count){
						if(oldbump[a].flag){
							newarray[c] = oldbump[a];
							a += 1;
							c += 1;
						}
						else
							a += 1;
					}
					while (b < runcount){
						if(sortedrun[b].flag){
							newarray[c] = sortedrun[b];
							b += 1;
							c += 1;
						}
						else
							b += 1;
					}

					free(oldbump);

					if ( c - oldrun.size > 0){
						char newname[FILE_NAME];
						sprintf(newname, LOC_FAST"data/L%dN%d", Current->next->number, (destlevel->count - 1));
						fp = fopen(newname, "wt");
						fwrite(newarray, sizeof(Node), oldrun.size, fp);
						fclose(fp);

						oldrun.count = oldrun.size;
						strcpy(oldrun.start , newarray[0].key);
						strcpy(oldrun.end , newarray[oldrun.size - 1].key);

						destlevel->array[destlevel->count - 1] = oldrun;

						char filename[FILE_NAME];
						sprintf(filename, LOC_FAST"data/L%dN%d", (origin+1), destlevel->count);
						FILE *fpw = fopen(filename, "wt");
						fwrite(&newarray[oldrun.size], sizeof(Node), (c - oldrun.size), fpw);
						fclose(fpw);

						InsertRun(lsm,destlevel, (c - oldrun.size), oldrun.size, 
								newarray[oldrun.size].key, newarray[c - 1].key);
					}
					else{
						char newname[FILE_NAME];
						sprintf(newname, LOC_FAST"data/L%dN%d", Current->next->number, (destlevel->count - 1));
						fp = fopen(newname, "wt");
						fwrite(newarray, sizeof(Node), c, fp);
						fclose(fp);

						oldrun.count = c;
						strcpy(oldrun.start , newarray[0].key);
						strcpy(oldrun.end , newarray[c - 1].key);

						destlevel->array[destlevel->count - 1] = oldrun;

					}
					free(newarray);
				}
			}
		}
		else{
			//겹치는 키가 있음
			int oldcount = 0;
			for(i = 0; i < j; i++){
				oldcount += destlevel->array[overlap[i]].count;
			}

			Node *oldarray = (Node *) malloc(oldcount * sizeof(Node));

			oldcount = 0;
			for(i = 0; i < j; i++){
				char name[FILE_NAME];
				sprintf(name, LOC_FAST"data/L%dN%d", Current->next->number, overlap[i]);
				FILE *fp = fopen(name, "rt");
				fread(&oldarray[oldcount], sizeof(Node), destlevel->array[overlap[i]].count, fp);
				fclose(fp);
				oldcount += destlevel->array[overlap[i]].count;
			}
			//		for(i = 0; i < j; i++){

			qsort((Node *)oldarray, oldcount,sizeof(Node), cmpfunc);

			Node *newarray = (Node *) malloc((oldcount + runcount) * sizeof(Node));
			int a = 0;
			int b = 0;
			int c = 0;
			while((a < oldcount) && (b < runcount)){
				if(strcmp(oldarray[a].key , sortedrun[b].key) < 0){
					if(oldarray[a].flag){
						newarray[c] = oldarray[a];
						a += 1;
						c += 1;
					}
					else
						a += 1;
				}else if(strcmp(oldarray[a].key , sortedrun[b].key) > 0 ){
					if(sortedrun[b].flag){
						newarray[c] = sortedrun[b];
						b += 1;
						c += 1;
					}
					else
						b += 1;
				}else{
					if(sortedrun[b].flag){
						newarray[c] = sortedrun[b];
						a += 1;
						b += 1;
						c += 1;
					}
				/*	else if(oldarray[a].flag){
						newarray[c] = oldarray[a];
						a += 1;
						b += 1;
						c += 1;
					}*/
					else{
						a += 1;
						b += 1;
					}

				}
			}
			while(a < oldcount){
				if(oldarray[a].flag){
					newarray[c] = oldarray[a];
					a += 1;
					c += 1;
				}
				else
					a+= 1;
			}
			while (b < runcount){
				if(sortedrun[b].flag){
					newarray[c] = sortedrun[b];
					b += 1;
					c += 1;
				}
				else
					b += 1;
			}

			int numrun;
			if(c % destlevel->array[0].size == 0){
				numrun = c / destlevel->array[0].size;
			}else{
				numrun = c / destlevel->array[0].size + 1;
			}
			if(numrun <= j){
				//중복된 값을 담고 있을 수 있으므로 담고 있기!
				for(i = 0; (i < (numrun - 1)); i++){
					Run oldrun = destlevel->array[overlap[i]];
					char name[FILE_NAME];
					sprintf(name, LOC_FAST"data/L%dN%d", Current->next->number, overlap[i]);
					FILE *fp = fopen(name, "wt");
					fwrite(&newarray[i * oldrun.size], sizeof(Node), oldrun.size, fp);
					fclose(fp);


					oldrun.count = oldrun.size;
					strcpy(oldrun.start , newarray[i * oldrun.size].key);
					strcpy(oldrun.end , newarray[(i + 1) * oldrun.size - 1].key);
					destlevel->array[overlap[i]] = oldrun;
				}
				Run oldrun = destlevel->array[overlap[numrun - 1]];
				char name[FILE_NAME];
				sprintf(name, LOC_FAST"data/L%dN%d", Current->next->number, overlap[numrun - 1]);
				FILE *fp = fopen(name, "wt");
				fwrite(&newarray[(numrun - 1) * oldrun.size], sizeof(Node), (c - (numrun - 1) * oldrun.size), fp);
				fclose(fp);


				oldrun.count = c - (numrun - 1) * oldrun.size;
				strcpy(oldrun.start , newarray[(numrun - 1) * oldrun.size].key);
				strcpy(oldrun.end , newarray[c - 1].key);
				destlevel->array[overlap[numrun - 1]] = oldrun;

				if(numrun < j){
					for(i = numrun; i < j; i++){
						oldrun = destlevel->array[overlap[i]];
						oldrun.count = 0;
						strcpy(oldrun.start , "fffffffffe");
						strcpy(oldrun.end , "fffffffff");
						destlevel->array[overlap[i]] = oldrun;
					}
				}
			}else{
				for(i = 0; i < j; i++){
					Run oldrun = destlevel->array[overlap[i]];
					char name[FILE_NAME];
					sprintf(name, LOC_FAST"data/L%dN%d", Current->next->number, overlap[i]);
					FILE *fp = fopen(name, "wt");
					fwrite(&newarray[i * oldrun.size], sizeof(Node), oldrun.size, fp);
					fclose(fp);

					oldrun.count = oldrun.size;
					strcpy(oldrun.start , newarray[i * oldrun.size].key);
					strcpy(oldrun.end , newarray[(i + 1) * oldrun.size - 1].key);
					destlevel->array[overlap[i]] = oldrun;
				}

				if(destlevel->count == destlevel->size){
					Run pushtonext = PopRun(lsm,destlevel);
					Node *topush = (Node *) malloc(pushtonext.count * sizeof(Node));
					char name[FILE_NAME];
					sprintf(name, LOC_FAST"data/L%dN%d", Current->next->number, destlevel->count);
					FILE *fp = fopen(name, "rt");
					fread(topush, sizeof(Node), pushtonext.count, fp);
					fclose(fp);
					Merge(lsm,Current->next, (origin + 1), levelsize, 
							pushtonext.count, (pushtonext.size * (levelsize + 1)), topush, (targetfpr * (levelsize + 1)));
				}

				char filename[FILE_NAME];
				sprintf(filename, LOC_FAST"data/L%dN%d", (origin+1), destlevel->count);
				Run oldrun = destlevel->array[0];
				FILE *fp = fopen(filename, "wt");
				fwrite(&newarray[j * oldrun.size], sizeof(Node), (c - j*oldrun.size), fp);
				fclose(fp);

				InsertRun(lsm,destlevel, (c - j * oldrun.size), oldrun.size,
						newarray[j * oldrun.size].key, newarray[c- 1].key);
			}
			free(newarray);
			free(oldarray);
		}
		free(overlap);
		free(distance);
		pthread_rwlock_unlock(&destlevel->level_lock);
	}
	free(start);
	free(end);
	printf("merge done\n");
}

