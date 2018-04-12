.PHONY: default setup clean build 08 all

default: build

build:
	jbuilder build

setup:
	opam install --deps-only ./pm.opam

clean:
	jbuilder clean

