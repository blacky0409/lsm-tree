#include "global.h"

#define INT_MAX 2147483647

int dead = 0;

LSMtree *CreateLSM(int buffersize, int sizeratio, double fpr){
	LSMtree *lsm = (LSMtree *) malloc(sizeof(LSMtree));
	if(lsm == NULL){
		printf("There is not enough memory for an LSM-tree.");
		return NULL;
	}
	lsm->buffer = CreateHeap(buffersize);
	lsm->T = sizeratio;
	lsm->L0 = (LevelNode *) malloc(sizeof(LevelNode));
	lsm->L0->level = NULL;
	lsm->L0->number = 0;
	lsm->L0->next = NULL;
	lsm->fpr1 = fpr; //delete
	pthread_mutex_init(&(lsm->lock), NULL);
	return lsm;
}

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

void Merge(LevelNode *Current, int origin, int levelsize,
		int runcount, int runsize, Node *sortedrun, double targetfpr){
	char * start = (char *) malloc(sizeof(char) * STRING_SIZE);
	char * end = (char *) malloc(sizeof(char) * STRING_SIZE);

	if(Current->next == NULL){ //no level
		Current->next = (LevelNode *) malloc(sizeof(LevelNode));
		Current->next->level = CreateLevel(levelsize, targetfpr);
		strcpy(start,sortedrun[0].key);
		strcpy(end, sortedrun[runcount - 1].key);

		runcount = Delete_flag(sortedrun, runcount);
		char filename[14];
		sprintf(filename, "data/L%dN%d", (origin+1), Current->next->level->count);
		FILE *fp = fopen(filename, "wt");
		if(fp == NULL){
			fprintf(stderr, "Couldn't open %s: %s\n", filename, strerror(errno));	
		}
		fwrite(sortedrun, sizeof(Node), runcount, fp);
		fclose(fp);
		InsertRun(Current->next->level, runcount, runsize, start, end);
		Current->next->number = origin + 1;
		Current->next->next = NULL;

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

				char filename[14];
				sprintf(filename, "data/L%dN%d", (origin+1), destlevel->count);
				FILE *fp = fopen(filename, "wt");
				fwrite(sortedrun, sizeof(Node), runcount, fp);
				fclose(fp);

				InsertRun(destlevel, runcount, runsize, start, end);
			}else{
				if(minpos != -1){
					Run oldrun = destlevel->array[minpos];
					Node *newarray = (Node *) malloc((oldrun.count + runcount) * sizeof(Node));

					Node *oldbump = (Node *) malloc(oldrun.count * sizeof(Node));
					char name[14];
					sprintf(name, "data/L%dN%d", Current->next->number, minpos);
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

					sprintf(name, "data/L%dN%d", Current->next->number, minpos);
					fp = fopen(name, "wt");
					fwrite(newarray, sizeof(Node), (c), fp);
					fclose(fp);

					oldrun.count = c;
					strcpy(oldrun.start , newarray[0].key);
					strcpy(oldrun.end , newarray[c - 1].key);
					destlevel->array[minpos] = oldrun;
					free(newarray);
				}else{
					Run pushtonext = PopRun(destlevel);
					Node *topush = (Node *) malloc(pushtonext.count * sizeof(Node));

					char name[15];
					sprintf(name, "data/L%dN%d", Current->next->number, destlevel->count);
					FILE *fp = fopen(name, "rt");
					fread(topush, sizeof(Node), pushtonext.count, fp);
					fclose(fp);

					Merge(Current->next, (origin + 1), levelsize, 
							pushtonext.count, pushtonext.size * (levelsize + 1), topush, (targetfpr * (levelsize + 1)));


					Run oldrun = destlevel->array[destlevel->count -1];
					Node *newarray = (Node *) malloc((oldrun.count + runcount) * sizeof(Node));


					Node *oldbump = (Node *) malloc(oldrun.count * sizeof(Node));
					sprintf(name, "data/L%dN%d", Current->next->number, (destlevel->count - 1));
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
					char newname[14];
					sprintf(newname, "data/L%dN%d", Current->next->number, (destlevel->count - 1));
					fp = fopen(newname, "wt");
					fwrite(newarray, sizeof(Node), oldrun.size, fp);
					fclose(fp);

					oldrun.count = oldrun.size;
					strcpy(oldrun.start , newarray[0].key);
					strcpy(oldrun.end , newarray[oldrun.size - 1].key);

					destlevel->array[destlevel->count - 1] = oldrun;

					char filename[14];
					sprintf(filename, "data/L%dN%d", (origin+1), destlevel->count);
					FILE *fpw = fopen(filename, "wt");
					fwrite(&newarray[oldrun.size], sizeof(Node), (c - oldrun.size), fpw);
					fclose(fpw);

					InsertRun(destlevel, (c - oldrun.size), oldrun.size, 
							newarray[oldrun.size].key, newarray[c - 1].key);
					}
					else{
					char newname[14];
					sprintf(newname, "data/L%dN%d", Current->next->number, (destlevel->count - 1));
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
				char name[14];
				sprintf(name, "data/L%dN%d", Current->next->number, overlap[i]);
				FILE *fp = fopen(name, "rt");
				fread(&oldarray[oldcount], sizeof(Node), destlevel->array[overlap[i]].count, fp);
				fclose(fp);
				oldcount += destlevel->array[overlap[i]].count;
			}

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
					else if(oldarray[a].flag){
					newarray[c] = oldarray[a];
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
					char name[14];
					sprintf(name, "data/L%dN%d", Current->next->number, overlap[i]);
					FILE *fp = fopen(name, "wt");
					fwrite(&newarray[i * oldrun.size], sizeof(Node), oldrun.size, fp);
					fclose(fp);


					oldrun.count = oldrun.size;
					strcpy(oldrun.start , newarray[i * oldrun.size].key);
					strcpy(oldrun.end , newarray[(i + 1) * oldrun.size - 1].key);
					destlevel->array[overlap[i]] = oldrun;
				}
				Run oldrun = destlevel->array[overlap[numrun - 1]];
				char name[14];
				sprintf(name, "data/L%dN%d", Current->next->number, overlap[numrun - 1]);
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
					char name[14];
					sprintf(name, "data/L%dN%d", Current->next->number, overlap[i]);
					FILE *fp = fopen(name, "wt");
					fwrite(&newarray[i * oldrun.size], sizeof(Node), oldrun.size, fp);
					fclose(fp);

					oldrun.count = oldrun.size;
					strcpy(oldrun.start , newarray[i * oldrun.size].key);
					strcpy(oldrun.end , newarray[(i + 1) * oldrun.size - 1].key);
					destlevel->array[overlap[i]] = oldrun;
				}

				if(destlevel->count == destlevel->size){
					Run pushtonext = PopRun(destlevel);
					Node *topush = (Node *) malloc(pushtonext.count * sizeof(Node));
					char name[14];
					sprintf(name, "data/L%dN%d", Current->next->number, destlevel->count);
					FILE *fp = fopen(name, "rt");
					fread(topush, sizeof(Node), pushtonext.count, fp);
					fclose(fp);
					Merge(Current->next, (origin + 1), levelsize, 
							pushtonext.count, (pushtonext.size * (levelsize + 1)), topush, (targetfpr * (levelsize + 1)));
				}

				char filename[14];
				sprintf(filename, "data/L%dN%d", (origin+1), destlevel->count);
				Run oldrun = destlevel->array[0];
				FILE *fp = fopen(filename, "wt");
				fwrite(&newarray[j * oldrun.size], sizeof(Node), (c - j*oldrun.size), fp);
				fclose(fp);

				InsertRun(destlevel, (c - j * oldrun.size), oldrun.size,
						newarray[j * oldrun.size].key, newarray[c- 1].key);
			}
			free(newarray);
			free(oldarray);
		}
		free(overlap);
		free(distance);
	}
	free(start);
	free(end);
}

void Put(LSMtree *lsm, char * key, int value, bool flag,ValueLog *log){

	int loc;
	ValuePut(log,&loc, key, strlen(key) + 1 , value);
	ValueLog_sync(log);

	int position = GetKeyPos(lsm->buffer, key);
	if(position >= 0){
		lsm->buffer->array[position].value = loc;
		lsm->buffer->array[position].flag = flag;
	}else{
		if(lsm->buffer->count < lsm->buffer->size){
			InsertKey(lsm->buffer, key, loc, flag);
		}else if(lsm->buffer->count == lsm->buffer->size){
			int i;
			Node *sortedrun = (Node *) malloc(lsm->buffer->size * sizeof(Node));
			for(i = 0; i < lsm->buffer->size; i++){
				sortedrun[i] = PopMin(lsm->buffer);
			}
			Merge(lsm->L0, 0, (lsm->T - 1), 
					lsm->buffer->size, lsm->buffer->size, sortedrun, lsm->fpr1);
			InsertKey(lsm->buffer, key, loc, flag);
		}
	}

}

int Get_loc(LSMtree *lsm, char * key){
	int position = GetKeyPos(lsm->buffer, key);
	int i;
	if(position != -1){
		if(lsm->buffer->array[position].flag){
			int val = lsm->buffer->array[position].value;
			return val;
		}
	}else{
		LevelNode *current = lsm->L0->next;
		bool find = false;
		while(current != NULL){
			Level *exploringlevel = current->level;
			for(i = 0; i < exploringlevel->count; i++){
				if(strcmp(exploringlevel->array[i].start , key) <= 0 && strcmp(exploringlevel->array[i].end , key) >= 0){
					Node *currentarray = (Node *) malloc(exploringlevel->array[i].count * sizeof(Node));
					char filename[14];
					sprintf(filename, "data/L%dN%d", current->number, i);
					FILE *fp = fopen(filename, "rt");
					fread(currentarray, sizeof(Node), exploringlevel->array[i].count, fp);
					fclose(fp);
					int left = 0;
					int right = exploringlevel->array[i].count - 1;
					int mid = (left + right) / 2;
					if(strcmp(key , currentarray[left].key) == 0){
						if(currentarray[left].flag){
							int val = currentarray[left].value;
							return val;
						}else{
							return 0;
						}
					}else if(strcmp( key , currentarray[right].key) == 0){
						if(currentarray[right].flag){
							int val = currentarray[right].value;
							return val;
						}else{
							return 0;
						}
					}
					char old[10];
					strcpy(old,currentarray[left].key);
					while(left != mid){
						if(strcmp( key , currentarray[mid].key) == 0){
							if(currentarray[mid].flag){
								int val = currentarray[mid].value;
								return val;
							}else{
								return 0;
							}
						}else if(strcmp ( key , currentarray[mid].key) > 0){
							left = mid;
							mid = (left + right) / 2;
						}else{
							right = mid;
							mid = (left + right) / 2;
						}
					}
					free(currentarray);						

				}
			}
			if(find){
				break;
			}
			current = current->next;
		}
	}
	printf("Can't find key\n");
	return 0;
}

void Delete(LSMtree *lsm, char * key){
	int position = GetKeyPos(lsm->buffer, key);
	int i;
	if(position != -1){
		if(lsm->buffer->array[position].flag){
			lsm->buffer->array[position].flag = false;
			return;
		}
	}else{
		LevelNode *current = lsm->L0->next;
		bool find = false;
		while(current != NULL){
			Level *exploringlevel = current->level;
			for(i = 0; i < exploringlevel->count; i++){
				if(strcmp(exploringlevel->array[i].start , key) <= 0 && strcmp(exploringlevel->array[i].end , key) >= 0){
					Node *currentarray = (Node *) malloc(exploringlevel->array[i].count * sizeof(Node));
					char filename[14];
					sprintf(filename, "data/L%dN%d", current->number, i);
					FILE *fp = fopen(filename, "rt");
					fread(currentarray, sizeof(Node), exploringlevel->array[i].count, fp);
					fclose(fp);
					int left = 0;
					int right = exploringlevel->array[i].count - 1;
					int mid = (left + right) / 2;
					if(strcmp(key , currentarray[left].key) == 0){
						if(currentarray[left].flag){
							currentarray[left].flag = false;
							return;
						}else{
							return;
						}
					}else if(strcmp( key , currentarray[right].key) == 0){
						if(currentarray[right].flag){
							currentarray[right].flag = false;
							return ;
						}else{
							return ;
						}
					}
					char old[10];
					strcpy(old,currentarray[left].key);
					while(left != mid){
						if(strcmp( key , currentarray[mid].key) == 0){
							if(currentarray[mid].flag){
								currentarray[mid].flag = false;
								return;
							}else{
								return;
							}
						}else if(strcmp ( key , currentarray[mid].key) > 0){
							left = mid;
							mid = (left + right) / 2;
						}else{
							right = mid;
							mid = (left + right) / 2;
						}
					}
					free(currentarray);						

				}
			}
			if(find){
				break;
			}
			current = current->next;
		}
	}
	printf("Can't find key\n");
	return;
}

void Range(LSMtree *lsm, char * start, char * end,ValueLog *log){
	int i;
	int j;
	int find = 0;
	ValueLog_sync(log);
	HashTable *table = CreateHashTable(128);

	printf("range query result for [%s, %s) is ", start, end);
	char str[32];
	for(i = 0; i < lsm->buffer->count; i++){
		if((strcmp(lsm->buffer->array[i].key , start) >= 0) && (strcmp(lsm->buffer->array[i].key , end)) < 0){
			if(!CheckTable(table, lsm->buffer->array[i].key)){
				find += 1;
				AddToTable(table, lsm->buffer->array[i].key);
				if(lsm->buffer->array[i].flag){
					bzero(str, 32);
					sprintf(str, "%s:%ld ", lsm->buffer->array[i].key, ValueGet(log,lsm->buffer->array[i].value));
					printf("%s ",str);
				}
			}
		}
	}
	LevelNode *currentlevelnode = lsm->L0->next;
	while(currentlevelnode != NULL){
		int levelnum = currentlevelnode->number;
		for(i = 0; i < currentlevelnode->level->count; i++){
			if((strcmp(currentlevelnode->level->array[i].end , start) >= 0) || (strcmp(currentlevelnode->level->array[i].start , end) < 0)){
				Node *currentarray = (Node *) malloc(currentlevelnode->level->array[i].count * sizeof(Node));
				char filename[14];
				sprintf(filename, "data/L%dN%d", levelnum, i);
				FILE *fp = fopen(filename, "rt");
				fread(currentarray, sizeof(Node), currentlevelnode->level->array[i].count, fp);
				fclose(fp);
				for(j = 0; j < currentlevelnode->level->array[i].count; j++){
					if(strcmp(currentarray[j].key , end) >= 0){
						break;
					}else if(strcmp(currentarray[j].key , start) >= 0){
						if(!CheckTable(table, currentarray[j].key)){
							find += 1;
							AddToTable(table, currentarray[j].key);
							if(currentarray[j].flag){
								bzero(str, 32);
								sprintf(str, "%s:%ld ", currentarray[j].key, ValueGet(log,currentarray[j].value));
								printf("%s ",str);
							}
						}
					}
				}
				free(currentarray);
			}
		}
		currentlevelnode = currentlevelnode->next;
	}
	ClearTable(table);
}


void PrintStats(LSMtree *lsm, ValueLog *log){
	int i;
	int j;
	int total = 0;

	LevelNode *Current = lsm->L0;
	LevelNode *currentlevelnode = Current->next;

	while(currentlevelnode != NULL){
		int levelnum = currentlevelnode->number;
		int currentcount = 0;
		for(i = 0; i < currentlevelnode->level->count; i++){
			currentcount += currentlevelnode->level->array[i].count;
			Node *currentarray = (Node *) malloc(currentlevelnode->level->array[i].count * sizeof(Node));
			char filename[14];
			sprintf(filename, "data/L%dN%d", levelnum, i);
			FILE *fp = fopen(filename, "rt");
			fread(currentarray, sizeof(Node), currentlevelnode->level->array[i].count, fp);
			fclose(fp);
			for(j = 0; j < currentlevelnode->level->array[i].count; j++){
				printf("%s:%ld:L%d  ", currentarray[j].key, ValueGet(log,currentarray[j].value), levelnum);
				if(!currentarray[j].flag){
					total -= 1;
				}
			}
			printf("a run has ended. \n");
			free(currentarray);
		}
		printf("There are %d pairs on Level %d. \n\n", currentcount, levelnum);
		total += currentcount;
		currentlevelnode = currentlevelnode->next;
	}
	printf("There are %d pairs on the LSM-tree in total. \n", total);
}

int Get(LSMtree *lsm, char * key, ValueLog *log){
	return (ValueGet(log,Get_loc(lsm, key)));
}
int main(){
	LSMtree *lsm = CreateLSM(4, 4, 0.0000001);
	ValueLog *log = CreateLog(0,4000);
	srand((unsigned int) time(NULL));

	char Get_want[1000][10];
	int Get_re[1000];
	int index = 0;
	char input[10];
	int d = 0;
	for(int i=0; i < 1000 ; i++){

		//Make random key
		int w = 0;
		for(w = 0 ; w < 9; w++){
			input[w] = 'a' + rand() % 26;
		}

		input[w] = 0;
		//Make random value
		int key_value = rand()%1000 + 1;

		Put(lsm,input, key_value,true,log);

		if(i%5 == 0){
			if(5 <= d && d < 8)
			{
				Delete(lsm,input);
			}
			strcpy(Get_want[index],input);
			Get_re[index] = key_value;
			index++;
			d++;
		}

	}

	printf("\n\n");

	int return_val;
	int false_count = 0;
	for(int i = 0 ; i < index; i ++){
		return_val = Get(lsm, Get_want[i], log);
		char answer[10];
		strcpy(answer,(return_val == Get_re[i])? "true" : "false");
		printf("value of key %s is %d, answer : %s\n", Get_want[i],return_val, answer);
		if(strcmp(answer,"false") == 0){
			printf("\n\n");
			false_count++;
		}
		printf("\n");
	}

	char start[10] = "aaaaaaaaa\0";
	char end[10] = "ccccccccc\0";
	Range(lsm, start, end,log);

	printf("\n\n");

	PrintNode(lsm->buffer,log);
	printf("\n");
	PrintStats(lsm,log);

	ClearLog(log);

	printf("false count : %d, dead : %d\n",false_count,dead);
	return 0;
}

