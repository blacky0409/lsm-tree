#include "global.h"

#define REPEAT 5
#define INT_MAX 2147483647

int main(){
	LSMtree *lsm = CreateLSM(4, 4, 0.0000001);
	ValueLog *log = CreateLog(0,0);
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
		printf("%d : %s : %d\n",i,input,key_value);

		/*	strcpy(Get_want[i],input);
			Get_re[i] = key_value;
			if(5 <= d && d < 8)
			{
			Delete(lsm,input);
			}
			d++;*/
		if(i%5 == 0){
			if(5 <= d && d < 8)
			{
				Delete(lsm,input);
			}
			d++;
		/*	strcpy(Get_want[index],input);
			Get_re[index] = key_value;
			index++;
			d++;*/
		}
		strcpy(Get_want[index],input);
		Get_re[index] = key_value;
		index++;
		if(i!=0 && i%200 == 0){
			GC(lsm,log);
		}
	}


	int return_val;
	int false_count = 0;

	for(int i = 0 ; i < index; i ++){
		return_val = Get(lsm, Get_want[i], log);
		char answer[10];
		strcpy(answer,(return_val == Get_re[i])? "true" : "false");
		printf("%d : value of key %s is %d, answer : %s\n", i,Get_want[i],return_val, answer);
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

	PrintNode(lsm,log);
	printf("\n");
	PrintStats(lsm,log);

	/*
	for(int w=0; w < REPEAT; w++){
		GC(lsm,log);

		for(int i = 0 ; i < index; i ++){
			return_val = Get(lsm, Get_want[i], log);
			char answer[10];
			strcpy(answer,(return_val == Get_re[i])? "true" : "false");
			printf("%d : value of key %s is %d, answer : %s\n", i,Get_want[i],return_val, answer);
			if(strcmp(answer,"false") == 0){
				printf("\n\n");
				false_count++;
			}
			printf("\n");
		}
	}*/



	ClearLog(log);


	printf("false count : %d = %d\n",false_count,3 * REPEAT + 3);
	return 0;
}

