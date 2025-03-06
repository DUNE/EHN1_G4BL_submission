This repo holds a [justin]([url](https://justin.dune.hep.ac.uk/docs/)) jobscript and a python script to make metadata for H2 and H4 VLE beam line simulations. These files are then used as input to the larsoft-based ProtoDUNE-SP/HD/VD event generators.



The actual simulation uses G4Beamline(Link?) and configurations created by the CERN beam expert in charge of the 2 beam lines. Tarballs of the G4beamline binaries, the configurations, and associated cross section data can be found in 
`/exp/dune/data/users/calcuttj/G4Beamline/H4_G4BL_DUNE/Jake_testing/` respectively as the following tarballs:
<pre>
  g4bl.tar.gz 
  pack.tar.gz
  Geant4Data.tar.gz
</pre>

For now, these will have to be uploaded to the cvmfs-based Rapid Code Distributon Service (RCDS -- see [this link]([url](https://cdcvs.fnal.gov/redmine/projects/jobsub/wiki/Rapid_Code_Distribution_Service_via_CVMFS_using_Jobsub)) for using RCDS on the grid with jobsub) for justin jobs.  In the future, these will be distributed on cvmfs for ease of use.

