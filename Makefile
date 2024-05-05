CC= gcc
CFLAGS=-Wall -Wextra -fPIC -MMD
OBJS= value-log.o hashtable.o compaction.o heap.o level.o lsm-tree.o queue.o test.o
TARGET= test
PATH_FSMALLOC=/home/soccer1356/malloc/FSmalloc
DEPS=$(wildcard *.d)
INCLUDE=/home/soccer1356/lsmtree

%.o: %.c
	$(CC) -c -o $@ $< $(CFLAGS) -I$(PATH_FSMALLOC) -I$(INCLUDE)

debug: CFLAGS += -g
debug: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) -o $@ $^ $(CFLAGS) -L$(PATH_FSMALLOC) -L$(INCLUDE) -lpthread -lFSmalloc -lnuma

value-log.o: value-log.c global.h
hashtable.o: hashtable.c global.h
compaction.o: compaction.c global.h
heap.o: heap.c global.h
level.o: level.c global.h
lsm-tree.o: lsm-tree.c global.h
queue.o: queue.c global.h
test.o: test.c global.h


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

.PHONY: start
start:
	@./start

-include $(DEPS)
