This repo holds a [justin](https://justin.dune.hep.ac.uk/docs/) jobscript and a python script to make metadata for H2 and H4 VLE beam line simulations. These files are then used as input to the larsoft-based ProtoDUNE-SP/HD/VD event generators.



The actual simulation uses G4Beamline(Link?) and configurations created by the CERN beam expert in charge of the 2 beam lines. Tarballs of the G4beamline binaries, the configurations, and associated cross section data can be found in 
`/exp/dune/data/users/calcuttj/G4Beamline/H4_G4BL_DUNE/Jake_testing/` respectively as the following tarballs:
<pre>
  g4bl.tar.gz 
  pack.tar.gz
  Geant4Data.tar.gz
</pre>

For now, these will have to be uploaded to the cvmfs-based Rapid Code Distributon Service (RCDS -- see [this link](https://cdcvs.fnal.gov/redmine/projects/jobsub/wiki/Rapid_Code_Distribution_Service_via_CVMFS_using_Jobsub)) for using RCDS on the grid with jobsub) for justin jobs.  In the future, these will be distributed on cvmfs for ease of use.

# Submitting with justin

## Preparation
Note: must use SL7 container in order to use ups to set up justin

Set up ups etc: `source /cvmfs/dune.opensciencegrid.org/products/dune/setup_dune.sh`

Set up justin: `setup justin`

Make sure your token is active: `htgettoken -a htvaultprod.fnal.gov -i dune`

Make a tarball of the metadata script and upload to RCDS: `tar -cf input.tar make_g4bl_metadata.py; input_dir=$(justin-cvmfs-upload input.tar)`

Upload each of the g4bl-related tarballs:
<pre>
  jake_dir=/exp/dune/data/users/calcuttj/G4Beamline/H4_G4BL_DUNE/Jake_testing/
  g4bl_dir=$(justin-cvmfs-upload $jake_dir/g4bl.tar.gz)
  pack_dir=$(justin-cvmfs-upload $jake_dir/pack.tar.gz)
  g4data_dir=$(justin-cvmfs-upload $jake_dir/Geant4Data.tar.gz)
</pre>

## Jobscript arguments
Several arguments can be provided to the justin jobscript via setting environment variables. When running an interactive test or submitting via justin, 
these are provided to the job's environment using the following syntax: `--env ENVVAR=VALUE`

Full examples of using this syntax are shown below. For now, the following is a 
list of all of the environment variables that can be configured for the job. The last 4 entries are the environment variables corresponding to the 
tarballs which have been uploaded and unpacked to cvmfs. For now, these must be provided. The rest of the variables have defaults (so they don't have to be provided).


| Name  | Description | Default | 
| ------------- | ------------- | ------------- |
| POLARITY  | Polarity of beam line ("+" or "-") | + |
| BEAMLINE  | Which beam line to use ("H4" or "H2") | H4 |
| CENTRALP  | Momentum setting (in GeV) of VLE -- i.e. entering NP02/4 | 1.0 |
| NPART     | How many Particles to send at the target (Need at least 100) | 100 |
| G4DATA_DIR | Location of unpacked G4Data tarball | -- |
| G4BL_DIR | Location of unpacked g4bl tarball | -- |
| PACK_DIR | Location of unpacked "pack" tarball | -- |
| INPUT_DIR | Location of unpacked input tarball (contains make_g4bl_metadata.py) | -- |


## Example -- Interactive Test
The following will run an interactive test of the jobscript and will impart 1000 particles on the H2 target, using positive polarity settings, with a central momentum of 1.0 GeV/c
 <pre>
   justin-test-jobscript --jobscript g4beamline_justin.jobscript --monte-carlo 1 \
                         --env G4DATA_DIR=$g4data_dir/ --env G4BL_DIR=$g4bl_dir/ \
                         --env PACK_DIR=$pack_dir/ --env INPUT_DIR=$input_dir/  \
                         --env BEAMLINE=H2 --env NPART=1000
 </pre>
 This took about 2 minutes to run during a single test on one of the dunegpvms. When finished, the temp workdir where output files can be found is shown by justin. Note that 
 the actual directory will be different everytime, as a random hash is produced to create distinct locations.
<pre>
  ====End of jobscript execution====
/tmp/justin-test-jobscript.0mv8dl/home/workspace:
total 472
-rw------- 1 calcuttj dune 176696 Mar  6 16:38 g4bloutput.txt
-rw------- 1 calcuttj dune 283635 Mar  6 16:38 H2_v27c_1GeV_1_20250306T223828Z_000001.root
-rw------- 1 calcuttj dune    534 Mar  6 16:38 H2_v27c_1GeV_1_20250306T223828Z_000001.root.json
-rw------- 1 calcuttj dune      7 Mar  6 16:38 justin-processed-pfns.txt
</pre>

These temp directories are periodically cleaned up, so if you produce anything you'd like to continue using for testing, copy the output to your data directory `/exp/dune/data/users/${USER}/`
## Example -- Full Submission

The above command can be easily translated into a workflow submission by adding a few more arguments i.e. `--rss-mib`, `--max-distance`, `--output-pattern`, and `--wall-seconds`. 
Consult this [justin tutorial](https://justin.dune.hep.ac.uk/docs/tutorials.dune.md)) to learn what these mean and the other options available. 

<pre>
justin simple-workflow --jobscript g4beamline_justin.jobscript --monte-carlo 100 \
                       --env G4DATA_DIR=$g4data_dir/ --env G4BL_DIR=$g4bl_dir/ \
                       --env PACK_DIR=$pack_dir/ --env INPUT_DIR=$input_dir/  \
                       --env BEAMLINE=H2 --env NPART=100 --rss-mib 3999 \
                       --max-distance 30 \
                       --output-pattern "*root:https://fndcadoor.fnal.gov:2880/dune/scratch/users/calcuttj/g4beamline_prod/H2_test_full/" \
                       --wall-seconds 3600
</pre>
