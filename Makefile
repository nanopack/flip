./extention/mdb:
	git clone https://gitorious.org/mdb/mdb.git ./extention/mdb


all: ./extention/mdb
	make -C ./extention/mdb/libraries/liblmdb
