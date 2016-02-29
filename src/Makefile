
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
OPTIMIZATION?=-O2

# Default settings
STD=-std=c99
WARN=-Wall -W
OPT=$(OPTIMIZATION)
DEBUG=-g -ggdb

FINAL_CFLAGS=$(STD) $(WARN) $(OPT) $(CFLAGS) $(DEBUG)
FINAL_LDFLAGS=$(LDFLAGS) $(DEBUG)

ifeq ($(uname_S),Linux)
	FINAL_CFLAGS+= -lpthread -D_GNU_SOURCE
	FINAL_LDFLAGS+= -lpthread
endif

INFQ_CC=$(QUIET_CC)$(CC) $(FINAL_CFLAGS)
INFQ_AR=$(QUIET_AR)ar crv
INFQ_LD=$(QUIET_LINK)$(CC) $(FINAL_LDFLAGS)
INSTALL=$(QUIET_INSTALL)$(INSTALL)

CCCOLOR="\033[34m"
LINKCOLOR="\033[34;1m"
SRCCOLOR="\033[33m"
BINCOLOR="\033[37;1m"
MAKECOLOR="\033[32;1m"
ENDCOLOR="\033[0m"

ifndef V
QUIET_CC = @printf '    %b %b\n' $(CCCOLOR)CC$(ENDCOLOR) $(SRCCOLOR)$@$(ENDCOLOR) 1>&2;
QUIET_AR = @printf '    %b %b\n' $(LINKCOLOR)AR$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR) 1>&2;
QUIET_LINK = @printf '    %b %b\n' $(LINKCOLOR)LINK$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR) 1>&2;
QUIET_INSTALL = @printf '    %b %b\n' $(LINKCOLOR)INSTALL$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR) 1>&2;
endif

INFQ_LIB=libinfq.a
INFQ_TEST_BIN=infq_test
INFQ_DUMP_TEST_BIN=dump_test
INFQ_LOAD_TEST_BIN=load_test
INFQ_FILE_BLOCK_READER_BIN=file_block_reader
INFQ_OBJ=bg_job.o file_block.o file_block_index.o file_queue.o infq.o logging.o mem_block.o mem_queue.o offset_array.o utils.o sha1.o infq_bg_jobs.o

all: $(INFQ_TEST_BIN) $(INFQ_DUMP_TEST_BIN) $(INFQ_LOAD_TEST_BIN) $(INFQ_FILE_BLOCK_READER_BIN)

$(INFQ_LIB): $(INFQ_OBJ)
	$(INFQ_AR) $(INFQ_LIB) $(INFQ_OBJ) 1>&2

$(INFQ_TEST_BIN): $(INFQ_LIB) main.o
	$(INFQ_LD) -o $@ $^ $(INFQ_LIB)

$(INFQ_DUMP_TEST_BIN): $(INFQ_LIB) persistent_dump_test.o
	$(INFQ_LD) -o $@ $^ $(INFQ_LIB)

$(INFQ_LOAD_TEST_BIN): $(INFQ_LIB) persistent_load_test.o
	$(INFQ_LD) -o $@ $^ $(INFQ_LIB)

$(INFQ_FILE_BLOCK_READER_BIN): $(INFQ_LIB) file_block_reader.o
	$(INFQ_LD) -o $@ $^ $(INFQ_LIB)

%.o: %.c
	$(INFQ_CC) -c $<

clean:
	rm -rf $(INFQ_LIB) $(INFQ_TEST_BIN) $(INFQ_DUMP_TEST_BIN) $(INFQ_LOAD_TEST_BIN) $(INFQ_FILE_BLOCK_READER_BIN) *.o

.PHONY: clean

noopt:
	$(MAKE) OPTIMIZATION="-O0"

