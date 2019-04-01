# this makefile is tuned for Microsoft nmake build tool
# with no wildcard capture
# run test by `nmake test TEST=$target$ ADD=$addition_src$`

# target not colllide with actual file name
# careful not to override global variable
LOCAL_INCLUDE = /I ./include /I .
FLAG = /EHsc
TEST_FLAG = /link /subsystem:console
TEST_LIB = gtest.lib gtest_main.lib
DB_SRC = db/hash_trie_iterator.cc db/bin_logger.cc db/bin_logger_daemon.cc \
	db/hash_trie.cc db/persist_hash_trie.cc util/file.cc
NET_SRC = network/socket.cc network/client.cc network/client_impl.cc \
	network/server_impl.cc network/server.cc

build: 
  cl $(LOCAL_INCLUDE) $(DB_SRC) $(NET_SRC) $(FLAG) /LD /Feportal_db
.PHONY : build

unittest:
  cl $(LOCAL_INCLUDE) $(TEST_LIB) test/*.cc $(NET_SRC) $(DB_SRC) $(FLAG) $(TEST_FLAG)
.PHONY : unittest

customized_test:
  cl $(LOCAL_INCLUDE) $(TEST_LIB) test/$(TEST)_test.cc $(ADD) $(FLAG) $(TEST_FLAG)
.PHONY : customized_test

cs_sample:
  cl $(LOCAL_INCLUDE) sample/client_sample.cc $(NET_SRC) $(DB_SRC) $(FLAG)
  cl $(LOCAL_INCLUDE) sample/server_sample.cc $(NET_SRC) $(DB_SRC) $(FLAG)
.PHONY: cs_sample

clean:
  rm *.exe, *.obj
  rm test/*.obj
.PHONY : clean
