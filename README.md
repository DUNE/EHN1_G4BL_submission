This repo holds a [justin]([url](https://justin.dune.hep.ac.uk/docs/)) jobscript and a python script to make metadata for H2 and H4 VLE beam line simulations. These files are then used as input to the larsoft-based ProtoDUNE-SP/HD/VD event generators.



The actual simulation uses G4Beamline(Link?) and configurations created by the CERN beam expert in charge of the 2 beam lines. Tarballs of the G4beamline binaries, the configurations, and associated cross section data can be found in 
`/exp/dune/data/users/calcuttj/G4Beamline/H4_G4BL_DUNE/Jake_testing/` respectively as the following tarballs:
<pre>
  g4bl.tar.gz 
  pack.tar.gz
  Geant4Data.tar.gz
</pre>

For now, these will have to be uploaded to the cvmfs-based Rapid Code Distributon Service (RCDS -- see [this link]([url](https://cdcvs.fnal.gov/redmine/projects/jobsub/wiki/Rapid_Code_Distribution_Service_via_CVMFS_using_Jobsub)) for using RCDS on the grid with jobsub) for justin jobs.  In the future, these will be distributed on cvmfs for ease of use.

=== Submitting with justin ===
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
