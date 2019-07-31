.PHONY: default setup clean build 08 all

default: build

build:
	dune build

setup:
	opam install --deps-only ./pm.opam

clean:
	dune clean

bench:
	dune build src/profiling/bench.exe

compare:
	dune build src/profiling/compare.exe

exec_bench:
	dune exec src/profiling/bench.exe

exec_compare:
	dune exec src/profiling/compare.exe

test:
	dune runtest

covered_test:
	BISECT_ENABLE=YES dune runtest

report:
	cd _build/default && bisect-ppx-report -html ../../ src/tests/*.out
