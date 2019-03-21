# careful not to override global variable
LOCAL_INCLUDE = /I ./include /I .
FLAG = /EHsc
TEST_FLAG = /link /subsystem:console
TEST_LIB = gtest.lib gtest_main.lib

test: test/$(TEST)_test.cc
  cl $(LOCAL_INCLUDE) $(TEST_LIB) test/$(TEST)_test.cc $(ADD) $(FLAG) $(TEST_FLAG)
.PHONY : test

network_test:
  cl /I ./include /I . test/client_test.cc network/socket.cc network/client.cc /EHsc
  cl /I ./include /I . test/server_test.cc network/socket.cc network/server.cc /EHsc
.PHONY: network_test

build: $(FILE)
  cl $(LOCAL_INCLUDE) $(FILE) $(ADD) $(FLAG)
.PHONY : build
clean:
  rm *.exe, *.obj
  rm test/*.obj
.PHONY : clean
