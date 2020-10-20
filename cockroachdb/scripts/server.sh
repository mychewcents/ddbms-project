#! /bin/sh

h=$(hostname -s | cut -c5-6)
action=$1

if [ $1 == "help" ]
then
  echo
  echo
  echo "This is simple manual for the executable."
  echo
  echo
  echo "\"cdbserv\" accepts two different types of arguments:"
  echo "   - start : To start the CockroachDB server on the 0.0.0.0."
  echo "   - stop  : To stop the local running instance of the Cockroach DB."
  echo
  echo
  echo "Happy Running!"
elif [ $1 == "start" ]
then
  printf "**********\nStarting Cockroach DB node on : node${h}\n**********\n\n"
  if [ $h == 30 ]
  then
    cockroach start --insecure --store=/home/stuproj/cs4224m/crdb-node-files/node0 \
      --listen-addr=192.168.48.179:27000 \
      --http-addr=0.0.0.0:40000 \
      --join=192.168.48.179:27000,192.168.48.180:27000,192.168.48.181:27000,192.168.48.182:27000,192.168.48.183:27000 \
      --background

    # cockroach init --insecure --host=0.0.0.0:30000

  elif [ $h == 31 ]
  then
    cockroach start --insecure --store=/home/stuproj/cs4224m/crdb-node-files/node1 \
      --listen-addr=192.168.48.180:27000 \
      --http-addr=0.0.0.0:40000 \
      --join=192.168.48.179:27000,192.168.48.180:27000,192.168.48.181:27000,192.168.48.182:27000,192.168.48.183:27000 \
      --background

  elif [ $h == 32 ]
  then
    cockroach start --insecure --store=/home/stuproj/cs4224m/crdb-node-files/node2 \
      --listen-addr=192.168.48.181:27000 \
      --http-addr=0.0.0.0:40000 \
      --join=192.168.48.179:27000,192.168.48.180:27000,192.168.48.181:27000,192.168.48.182:27000,192.168.48.183:27000 \
      --background

  elif [ $h == 33 ]
  then
    cockroach start --insecure --store=/home/stuproj/cs4224m/crdb-node-files/node3 \
      --listen-addr=192.168.48.182:27000 \
      --http-addr=0.0.0.0:40000 \
      --join=192.168.48.179:27000,192.168.48.180:27000,192.168.48.181:27000,192.168.48.182:27000,192.168.48.183:27000 \
      --background

  elif [ $h == 34 ]
  then
    cockroach start --insecure --store=/home/stuproj/cs4224m/crdb-node-files/node4 \
      --listen-addr=192.168.48.183:27000 \
      --http-addr=0.0.0.0:40000 \
      --join=192.168.48.179:27000,192.168.48.180:27000,192.168.48.181:27000,192.168.48.182:27000,192.168.48.183:27000 \
      --background

  fi
  printf "\n**********\nStarted server on : node${h}\n**********\n"
elif [ $1 == "stop" ]
then
  printf "**********\nStopping the node : node${h}\n**********\n\n"
  cockroach quit --insecure --host=$(hostname -i):27000
  printf "\n**********\nStopped the node : node${h}\n**********\n"
elif [ $1 == "init" ]
then
  printf "**********\nInitializing the cluster : node${h}\n**********\n\n"
  cockroach init --insecure --host=$(hostname -i):27000
  printf "\n**********\nStopped the node : node${h}\n**********\n"
fi
