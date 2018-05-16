.PHONY: default setup clean build 08 all

default: build

build:
	jbuilder build

setup:
	opam install --deps-only ./pm.opam

clean:
	jbuilder clean

test:
	jbuilder runtest

covered_test:
	BISECT_ENABLE=YES jbuilder runtest

report:
	cd _build/default && bisect-ppx-report -html ../../ src/tests/*.out
