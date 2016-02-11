apt-get -y install git make gcc librados-dev autoconf automake libtool
git clone https://github.com/akheron/jansson.git
cd jansson/
autoreconf -vi
./configure
make
make check
make install
ldconfig
cd ../
make
