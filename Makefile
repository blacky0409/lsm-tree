CC= gcc
CFLAGS= -g -Wall
OBJS= lsm-tree.o level.o heap.o hashtable.o value-log.o
TARGET= lsm_tree.out

$(TARGET): $(OBJS)
	$(CC) -o $@ $(OBJS)

level.o: global.h level.c
heap.o: global.h heap.c
hashtable.o: global.h hashtable.c
value-log.o: global.h value-log.c
lsm-tree.o: lsm-tree.c global.h


.PHONY: clean
clean:
	   rm -f *.o
	   rm -f $(TARGET)
	   rm -f cscope.out tags nvmev.S

.PHONY: cscope
cscope:
		cscope -b -R
		ctags *.[ch]

.PHONY: tags
tags: cscope

.PHONY: format
format:
	clang-format -i *.[ch]

.PHONY: dis
dis:
	objdump -d -S nvmev.ko > nvmev.S
