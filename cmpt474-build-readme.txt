# commands for building and installing zeromq and zookeeper
# probably best to run these by hand, checking output
# however this file can be run as a script with
# source cmpt474-build-readme.txt 
# to build everything in the local directory
# pip commands should work as is

# prerequisites - assumes you have sudo properly set up 
sudo apt-get install python2.7 python2.7-dev python-pip curl wget

# zeromq download, build and install - if configure fails check errors for missing dependencies
curl -O http://download.zeromq.org/zeromq-4.0.5.tar.gz
# or
# wget http://download.zeromq.org/zeromq-4.0.5.tar.gz
tar xzf zeromq-4.0.5.tar.gz
cd zeromq-4.0.5
./configure && make && sudo make install
cd ..

# zookeeper 
# documentation including how to install as a stand alone for development:
# http://zookeeper.apache.org/
curl -O http://apache.mirror.iweb.ca/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz
# or 
# wget http://apache.mirror.iweb.ca/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz
tar xzf zookeeper-3.4.6.tar.gz

## zookeeper does not need to be built and once set up can be started with 
## to start in stand alone mode locally this is all you need to do
# cd zookeeper-3.4.6
# cp conf/zoo_sample.cfg conf/zoo.cfg
# bin/zkServer.sh start
## for debug use "start-foreground" and look for error messages

# other modules for using python
# provides the "zmq" python module
sudo pip install pyzmq
# will also install kazoo
sudo pip install zc.zk

