# careful not to override global variable
LOCAL_INCLUDE = /I ./include /I .
TEST_FLAG = /link /subsystem:console
TEST_LIB = gtest.lib gtest_main.lib

test: test/$(TEST)_test.cc
	cl $(LOCAL_INCLUDE) $(TEST_LIB) test/$(TEST)_test.cc $(TEST_FLAG)
.PHONY : test

clean:
	rm *.exe, *.obj
.PHONY : clean
