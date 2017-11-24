#!/bin/bash

export LC_ALL=C

# se obtiene el directorio desde donde se ejecuta el sh
SCRIPT=$(readlink -f $0);
dir_base=`dirname $SCRIPT`;

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
	cd $dir_base/shared-library/shared-library
	make clean
	make all

# master
	cd $dir_base/master/src
	make clean
	make all

# file-system
	cd $dir_base/filesystem/src
	make clean
	make all

# yama
	cd $dir_base/yama/src
	make clean
	make all

# worker
	cd $dir_base/worker/src
	make clean
	make all

# data-node
	cd $dir_base/datanode/src
	make clean
	make all
