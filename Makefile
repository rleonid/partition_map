.PHONY: default setup clean build

default: build

build:
	jbuilder build

setup:
	opam install --deps-only ./pm.opam

clean:
	jbuilder clean

