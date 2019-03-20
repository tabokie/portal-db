# careful not to override global variable
LOCAL_INCLUDE = /I ./include /I .
FLAG = /EHsc
TEST_FLAG = /link /subsystem:console
TEST_LIB = gtest.lib gtest_main.lib

test: test/$(TEST)_test.cc
	cl $(LOCAL_INCLUDE) $(TEST_LIB) test/$(TEST)_test.cc $(ADD) $(FLAG) $(TEST_FLAG)
.PHONY : test

clean:
	rm *.exe, *.obj
	rm test/*.obj
.PHONY : clean
