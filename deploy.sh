#!/bin/bash

export LC_ALL=C

# se descarga y se instala so-commons
	if [ -d "so-commons-library" ]; then rm -Rf so-commons-library; fi
	mkdir so-commons-library
	git clone https://dromero-7854:ASDzxc7854@github.com/sisoputnfrba/so-commons-library.git ./so-commons-library
	cd ./so-commons-library
	make
	sudo make install
	cd ..
	rm -Rf so-commons-library;

# se descarga y se instala readline
	sudo apt-get install libreadline6 libreadline6-dev

# shared-library
	cd /shared-library/shared-library
	make clean
	make all

# master
	cd /master/src
	make clean
	make all

# file-system
	cd /filesystem/src
	make clean
	make all

# yama
	cd /yama/src
	make clean
	make all

# worker
	cd /worker/src
	make clean
	make all

# data-node
	cd /datanode/src
	make clean
	make all