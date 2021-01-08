all: run

build:
	g++ main.cpp local_grader.cpp -O2 -std=c++11 -o main

run: build
	./main seq-read test_file
	rm -f test_file
	./main seq-write test_file
	rm -f test_file
	./main rnd-read test_file
	rm -f test_file
	./main rnd-write test_file
	rm -f test_file

run_direct_io: build
	./main seq-read test_file direct_io
	rm -f test_file
	./main seq-write test_file direct_io
	rm -f test_file
	./main rnd-read test_file direct_io
	rm -f test_file
	./main rnd-write test_file direct_io
	rm -f test_file

