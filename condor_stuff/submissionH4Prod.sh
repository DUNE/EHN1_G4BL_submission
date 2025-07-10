#!/bin/bash

export ONLYSUBMIT=$1

#Defaults
#export ACCGROUP="group_u_NP04.e_np04_t0comp_users"
export ACCGROUP="group_u_NP02.e_np02_t0comp_users"
#export ACCGROUP="group_u_NA61.u_wj"
#export ACCGROUP="group_u_NA65.users"
#export REQMEMORY=2000

export ADDPARAM=""
export ADDFILES=""

for iter in {991..1000..1}

do
	####
	#### PRODUCTION FOR COLLABORATION
	####
	export EXSCRIPT=RunG4Beamline.sh
	export INFILE=H4.in
	export TITLE=H4Prod_80000_newProd_${iter}
#	export TITLE=H4Prod_50000_s50_W400_${iter}
	export JOBS=1000
	export PARTPERJOB=10000
# REMEMBER TO PUT BACK TO NOMINAL 80000 ΜΕV/C 
	export PMOMENTUM=80000 
	export MOMLIST="3"
	export TIMELIMIT=$((${PARTPERJOB}*2+720))

	export EOSDIR=/eos/user/n/ncharito/H4_MARCEL/G4BLModel
	export EOSCONDDIR=/eos/user/n/ncharito/H4_Prod/${TITLE}

	if [ $ONLYSUBMIT -eq 0 ]
	then
	  mv $EOSCONDDIR ${EOSCONDDIR}_old
	  mkdir $EOSCONDDIR

	  CURDIR=$(pwd)
	  cd $EOSDIR
	  tar -czvf $EOSCONDDIR/pack.tar.gz *.in *.map;
	  cd $CURDIR

	  echo "Submitting..."
	  cd $CURDIR
	fi

	for energy in $MOMLIST
	do
	  WORKS=0
	  export MOMENTUMVLE=$energy
	  echo $MOMENTUMVLE
	  while [  $WORKS -lt 5 ]; do
		  WORKS=$(($WORKS+1))

		  export ADDPARAM="momentumVLE=$MOMENTUMVLE pMomentum=$PMOMENTUM"
		  #echo $ADDPARAM
		  condor_submit -batch-name S_${TITLE}_${MOMENTUMVLE} submission.sub
		  if [ $? -eq 0 ]
		  then
			WORKS=10
		  fi
		  if [ $WORKS -eq 5 ]
		  then
			echo "FAILED & Exiting..."
			rm -rv ${EOSCONDDIR}_old
			exit -1
		  fi
      if [ $WORKS -lt 8 ]
      then
        sleep 10
      fi
	  done
	done

	if [ $ONLYSUBMIT -eq 0 ]
	then
	  echo "Cleaning..."
	  rm -rv ${EOSCONDDIR}_old
	fi
done
