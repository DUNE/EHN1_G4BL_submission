#!/bin/bash

SECONDS=0

export JOBID=$(($st+1))

#############
#Unpack input files
#############

echo "Unpacking g4bl"
tar -xzf g4bl.tar.gz --checkpoint=1000
if [ $? -ne 0 ]
then
  echo "Exiting with error"
  exit 1
fi


echo "Unpacking Geant4Data"
tar -xzf Geant4Data.tar.gz --checkpoint=1000
if [ $? -ne 0 ]
then
  echo "Exiting with error"
  exit 1
fi
CURDIR=$(pwd)
echo $CURDIR/Geant4Data > g4bl/.data


echo "Unpacking Inputfiles Pack"
tar -xzf pack.tar.gz --checkpoint=1000
if [ $? -ne 0 ]
then
  echo "Exiting with error"
  exit 1
fi

#############
#Run G4beamline
#############

echo "Running G4beamline"
$CURDIR/g4bl/bin/g4bl $INFILE jobID=$JOBID totNumEv=$PARTPERJOB $ADDPARAM 2>&1 | tee g4bloutput.txt
#momentumVLE=$MOMENTUMVLE pMomentum=$PMOMENTUM detectorModelVersion=$DETMODEL valuesVLEquads=$VLEQUADS autoTarget=$AUTOTARG pipeMaterial=$PIPEMATERIAL targMat=$TARGETMATERIAL targetThickness=$TARGTHICK beamH2=$BEAMH2 beamH4=$BEAMH4
if [ $? -ne 0 ]
then
  echo "Exiting with error"
  exit 1
fi

errorsSaving=$((`cat g4bloutput.txt | grep "Error in <T" | wc -l`))
if [ $errorsSaving -ne 0 ]
then
  echo "Exiting with error"
  exit 1
fi

#############
#Copy Result to eos
#############

RC=$?
if [ $RC -ne 0 ]
then
  echo "Exiting with error"
  exit 1
fi

echo "Copy back to eos $EOSCONDDIR"
RC=1
COU=0
while [[ $COU -lt 3 && $RC -ne 0 ]]
do
  COU=$(($COU+1))
  rsync -avhW --progress --size-only --timeout=3600 *.root $EOSCONDDIR/
  RC=$?
  if [ $RC -ne 0 ]
  then
    sleep 60
  fi
done

if [ $RC -ne 0 ]
then
  echo "Exiting with error"
  exit 1
fi

echo "RUNTIME: $SECONDS seconds elapsed."
