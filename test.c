#include "global.h"

#define REPEAT 5
#define INT_MAX 2147483647

typedef struct test_art{
	LSMtree *lsm;
	ValueLog *log;
}Test_Arg;
void *GC_function(void *argument){
	Test_Arg * arg = (Test_Arg *)argument; 
	LSMtree * lsm = arg->lsm;
	ValueLog *log = arg->log;

	while(1){
		sleep(0.5);
		if(log->fast->utili >= UTILIZATION){
			GC(lsm,log);
		}
	}
}

void *thread_function(void *argument){
	Test_Arg * arg = (Test_Arg *)argument; 
	LSMtree * lsm = arg->lsm;
	ValueLog *log = arg->log;

	char Get_want[1000][10];
	int Get_re[1000];
	int index = 0;
	char input[10];

	srand(time(NULL));
	int ran;
	for(int i=0; i < 200 ; i++){
		//Make random key
		printf("%d번째\n",i);
		int w = 0;
		for(w = 0 ; w < 9; w++){
			input[w] = 'a' + rand() % 26;
		}

		input[w] = 0;
		//Make random value
		int key_value = rand()%1000 + 1;

		//ran = rand()%4 +1;
		sleep(1);
		Put(lsm,input, key_value,true,log);

		strcpy(Get_want[index],input);
		Get_re[index] = key_value;
		index++;
	}

	int return_val;
	int false_count = 0;

	for(int i = 0 ; i < index; i ++){
		return_val = Get(lsm, Get_want[i], log);
		char answer[10];
		strcpy(answer,(return_val == Get_re[i])? "true" : "false");
		printf("%d : value of key %s is %d, answer : %d,  %s\n", i,Get_want[i],return_val, Get_re[i],answer);
		if(strcmp(answer,"false") == 0){
			false_count++;
		}
	}

	printf("test false count : %d\n",false_count);
	int *ret = (int *)malloc(sizeof(int));

	*ret = false_count;

	return (void *)ret;

}
int main(){
	LSMtree *lsm = CreateLSM(10, 10, 0.0000001); //50
	ValueLog *log = CreateLog(0,0);
	srand((unsigned int) time(NULL));
	int d = 0;

	pthread_t thread[5];
	pthread_t GC_thread;

	
	Test_Arg * arg = (Test_Arg *)malloc(sizeof(Test_Arg));
	arg->lsm = lsm;
	arg->log = log;


	for(int i = 0 ; i < (sizeof(thread) / sizeof(thread[0])); i++){
		int r = pthread_create(&thread[i],NULL, thread_function,arg);
		if(r == -1){
			printf("falut create thread\n");
			return 0;
		}
	}
	int r = pthread_create(&GC_thread,NULL, GC_function,arg);
	if(r == -1){
		printf("falut create thread\n");
		return 0;
	}

	/*	for(int i=0; i < 1000 ; i++){
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

	strcpy(Get_want[i],input);
	Get_re[i] = key_value;
	if(5 <= d && d < 8)
	{
	Delete(lsm,input);
	}
	d++;
	if(i%5 == 0){
	if(5 <= d && d < 8)
	{
	Delete(lsm,input);
	}
	d++;
	strcpy(Get_want[index],input);
	Get_re[index] = key_value;
	index++;
	d++;
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
	}*/

	int *ret;
	int sum = 0;
	for (int i = 0; i < (sizeof(thread) / sizeof(thread[0])); i++) {
		int r = pthread_join(thread[i], (void **)&ret);
		if(r == -1){
			printf("falut wait exit thread\n");
			return 0;
		}
		sum += *ret;
	}

	/*	char start[10] = "aaaaaaaaa\0";
		char end[10] = "ccccccccc\0";
		Range(lsm, start, end,log);

		printf("\n\n");

		PrintNode(lsm,log);
		printf("\n");
		PrintStats(lsm,log);*/

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

	printf("false count : %d\n",sum);
	ClearLog(log);


	return 0;
}

