from metacat.webapi import MetaCatClient
mc = MetaCatClient()

from rucio.client.replicaclient import ReplicaClient
rc = ReplicaClient(account='justinreadonly')

from argparse import ArgumentParser as ap
#from datetime import datetime
import datetime

import subprocess, os, json, tarfile
import time


def finish_metadata(args, outname, results):
    print('Finishing metadata')
    results['size'] = os.path.getsize(outname)

    if not args.skip_checksum:
      proc = subprocess.run(['xrdadler32', outname], capture_output=True)
      if proc.returncode != 0:
        raise Exception('xrdadler32 failed', proc.returncode, proc.stderr)

      checksum = proc.stdout.decode('utf-8').split()[0]
      results['checksums'] = {'adler32':checksum}

    return results


def do_merge(args):
  files = query(args)

  unique = [
    'beam.momentum',
    'beam.polarity',
    'core.data_stream',
    'core.data_tier',
    'core.file_format',
    'core.file_type',
    'core.group',
    'core.run_type',
    'dune.output_status',
    'retention.class',
    'retention.status',
  ]

  output_metadata = {
  }

  ##TODO -- run and subrun

  output_name = args.o if args.o != 'inherit' else ''

  print('Getting names and metadata')
  names = []
  for count, f in enumerate(files):
    if count == 0 and args.o == 'inherit':
      output_name = f['name'].split('/')[-1]
      output_name = '_'.join(output_name.split('_')[:5])
      output_name += f'_{args.run}_{args.subrun}.root'

    #if count == 0 and args.namespace is None:
    #  output_namespace = f['namespace']
    names.append({'scope':f['namespace'], 'name':f['name']})
    metadata = f['metadata']
    for k in unique:
      if k not in output_metadata:
        output_metadata[k] = metadata[k]
      else:
        if output_metadata[k] != metadata[k]:
          raise Exception(
            f'Found differing metadata values for key {k}'
            f'\n\tPrev val: {output_metadata[k]}'
            f'\n\tNew val: {metadata[k]}'
          )
  print('done')
  
  output_metadata['core.runs'] = [args.run]
  output_metadata['core.runs_subruns'] = [int(args.run*1e5 + args.subrun)]

  print(output_metadata)

  print('Getting paths from rucio')
  #stamp = int(time.time()*100)
  stamp = datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y%m%dT%H%M%S') 

  output_name = output_name.replace('.root', f'_{stamp}.root')
  #if args.namespace is not None:
  #  output_namespace = args.namespace
  #output_metadata['dune.dataset_name'] = f'{output_namespace}:{output_namespace}_g4bl_merged_{args.iteration}'

  #reps = rc.list_replicas(names, '''rse_expression='|'.join(args.rses)''')
  reps = rc.list_replicas(names, schemes=['root'])
  paths = []
  inputnames = []
  parents = []
  for rep in reps:
      #print(rep)
      if len(list(rep['pfns'].keys())) == 0:
          print(f'\tWarning: skipping file with no pfn {rep["scope"]}:{rep["name"]}')
          #if args.save_no_rse:
          #    with open(f'no_rse_{r}_{stamp}.txt', 'a') as f_no_rse:
          #        f_no_rse.write(f'{rep["scope"]}:{rep["name"]}\n')
          continue
      paths.append(list(rep['pfns'].keys())[0])
      inputnames.append(f'{rep["name"]}\n')
      parents.append(
        {'namespace':rep['scope'], 'name':rep['name']}
      )

  print(f'Got {len(paths)} paths from {len(names)} files')

  cmd = ['hadd', output_name] + paths
  print(cmd)

  if not args.dry_run:
    result = subprocess.run(cmd)
    if result.returncode != 0:
      raise Exception('Error in hadd')

    results = finish_metadata(
      args,
      output_name,
      {'metadata':output_metadata}
    )

    results['parents'] = parents

    json_object = json.dumps(results, indent=2)
    with open(f'{output_name}.json', 'w') as fjson:
      fjson.write(json_object)
    
    with open(f'{output_name}_inputs.txt', 'w') as ftext:
      ftext.writelines(inputnames)


def check_parents(args):
  from termcolor import colored

  print('Checking jobs')
  files = query(args, with_parents=True)
  print('Got files')

  all_fids = []
  for f in files:
    all_mcids = []
    parents = f['parents']
    parent_fids = []
    for p in parents:
      all_fids.append(p['fid'])
      parent_fids.append(p['fid'])
      # mc_id = mc.get_file(fid=p['fid'])['name'].replace('.root', '').split('_')[-1]
      # if mc_id in all_mcids:
      #   print(colored(f'Found duplicate MC ID {mc_id}', 'red'))
      # all_mcids.append(mc_id)

    if len(set(parent_fids)) != len(parent_fids):
      print(colored(f"Duplicate parent detected in file {f['fid']}", 'red'))

      
  print('All parents:', len(all_fids))
  print('Unique parents:', len(set(all_fids)))
  stamp = datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y%m%dT%H%M%S') 
  outname = f'{args.dataset.replace(":", "_")}_inputs_{stamp}.txt'
  print('Saving', outname)
  with open(outname, 'w') as f:
    f.writelines([l+'\n' for l in list(set(all_fids))])
  
def check_inputs(args):
  from termcolor import colored

  print('Checking jobs')
  files = query(args)
  print('Got files')
  all_inputs = []

  input_list = [{'scope':f['namespace'], 'name':f['name']+'_inputs.txt'} for f in files]
  
  # for f in input_list: f['name'] = f['name'] + '_inputs.txt'
  # print(f'Checking {input_list}')
  reps = rc.list_replicas(input_list, schemes=['root'])

  paths = []
  for rep in reps:
    if len(list(rep['pfns'].keys())) == 0:
        print(f'\tWarning: skipping file with no pfn {rep["scope"]}:{rep["name"]}')
        #if args.save_no_rse:
        #    with open(f'no_rse_{r}_{stamp}.txt', 'a') as f_no_rse:
        #        f_no_rse.write(f'{rep["scope"]}:{rep["name"]}\n')
        continue
    paths.append(list(rep['pfns'].keys())[0])
  print(f'Got {len(paths)} paths from {len(input_list)} files')

  for ifile, f in enumerate(paths):
    if 'otter' in f: continue
    print(f'Checking {ifile} {f}')

    try:
      fin = open(f, 'r')

    # with open(f, 'r') as fin:
      these_inputs = [l for l in fin.readlines()]
      if len(these_inputs) != 10:
        print(colored(f'Warning, found unexpected number of inputs {len(these_inputs)} {f.split("/")[-1]}', 'yellow'))
      all_inputs += these_inputs
    except:
      print('blah')
  print(f'Got {len(all_inputs)} inputs')
  print(f'Got {len(set(all_inputs))} unique inputs')

  stamp = datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y%m%dT%H%M%S') 
  outname = f'{args.dataset.replace(":", "_")}_inputs_{stamp}.txt'
  print('Saving', outname)
  with open(outname, 'w') as f:
    f.writelines([l for l in list(set(all_inputs))])

# def do_check(args):
#   print('Checking jobs')
#   files = query(args)
#   lognames = []
#   for f in files:
#     jobid = f['metadata']['dune.workflow']['job_id']
#     logname = jobid.replace('@', '-') + '.logs.tgz'
#     print("Found log:", logname)
#     lognames.append({'scope':'justin-logs', 'name':logname})

#   reps = rc.list_replicas(lognames, schemes=['root'])
#   paths = []
#   for rep in reps:
#       #print(rep)
#       if len(list(rep['pfns'].keys())) == 0:
#           print(f'\tWarning: skipping file with no pfn {rep["scope"]}:{rep["name"]}')
#           #if args.save_no_rse:
#           #    with open(f'no_rse_{r}_{stamp}.txt', 'a') as f_no_rse:
#           #        f_no_rse.write(f'{rep["scope"]}:{rep["name"]}\n')
#           continue
#       paths.append(list(rep['pfns'].keys())[0])
#       print(paths[-1])
#   print(f'Got {len(paths)} paths from {len(lognames)} files')
  
  
#   local_logs = [p.split('/')[-1] for p in paths]
#   for path in paths:
#     print('Copying', path)
#     ret = subprocess.run(['xrdcp', path, '.'])
#     if ret.returncode != 0:
#       raise Exception('Error copying', path)

#     log = path.split('/')[-1]

#     logtar = tarfile.open(log)

#     found_joblog = False
#     for f in logtar.getnames():
#       if 'jobscript.log' in f:
#         found_joblog = True
#         joblog = logtar.extractfile(logtar.getmember(f))
#         break
    
#     if not found_joblog: raise Exception('Could not find jobscript in ', log)

#     logstrs = joblog.read().decode('utf-8').split('\n')
#     found_error = False
#     for l in logstrs:
#       if 'hadd Source' in l:
#         print(l)
#       if 'Error in <TFileMerger::AddFile>' in l:
#         found_error = True
#         print('FOUND ERROR IN', log, 'KEEPING LOG TARFILE')
#     if not found_error: os.remove(log)

def query(args, with_parents=False):
  to_skip = args.skip if args.iter is None else args.iter*args.limit

  query = (
        f'files from {args.dataset} where dune.output_status=confirmed'
        f' ordered skip {to_skip} limit {args.limit}'
  )
  print(f'Querying {args.dataset} for {args.limit} files')
  print(f'Query: {query}')
  files = mc.query(query, with_metadata=True, with_provenance=with_parents)
  return files

def get_webpage(ifile, jobid):
    url = f'https://justin.dune.hep.ac.uk/dashboard/?method=show-job&jobsub_id={jobid}'
    print(colored(f'Downloading {ifile} {jobid}', 'green'))
    ret = subprocess.run(['wget', url], capture_output=True)
    fname = f'index.html?method=show-job&jobsub_id={jobid}'
    with open(fname, 'r') as flog:
      return fname, flog.readlines()

def get_justinlog(paths, jobid, ifile):
  if jobid not in paths.keys():
    print(colored(f'{jobid} not in paths', 'yellow'))
    return None, None
  path = paths[jobid]
  print(colored(f'Copying {ifile} {jobid}', 'green'))
  ret = subprocess.run(['xrdcp', path, '.'], capture_output=True)
  if ret.returncode != 0:
    # print(ret.stdout)
    raise Exception('Error copying', path)

  log = path.split('/')[-1]

  logtar = tarfile.open(log)

  found_joblog = False
  for f in logtar.getnames():
    if 'jobscript.log' in f:
      found_joblog = True
      joblog = logtar.extractfile(logtar.getmember(f))
      break
  
  if not found_joblog: raise Exception('Could not find jobscript in ', log)

  logstrs = joblog.read().decode('utf-8').split('\n')
  return log, logstrs


def do_check(args):
  from termcolor import colored

  print('Checking jobs')
  files = query(args)
  print('Got files')
  lognames = []
  all_inputs = []
  expected_inputs = 0
  jobids = []

  for ifile, f in enumerate(files):
    jobid = f['metadata']['dune.workflow']['job_id']
    jobids.append(jobid)

  #Iterate over the jobids, make lognames, and get the replcas from rucio
  if not args.use_web:
    lognames = []
    for jobid in jobids:
      logname = jobid.replace('@', '-') + '.logs.tgz'
      print("Found log:", logname)
      lognames.append({'scope':'justin-logs', 'name':logname})

    reps = rc.list_replicas(lognames, schemes=['root'])
    paths = {}
    for rep in reps:
        if len(list(rep['pfns'].keys())) == 0:
            print(colored(f'\tWarning: skipping file with no pfn {rep["scope"]}:{rep["name"]}', 'yellow'))
            continue
        # print(rep['pfns'])
        path = list(rep['pfns'].keys())[0]
        if 'otter' in path or 'xrootd-archive.cr.cnaf.infn' in path: continue
        jobid = path.split('/')[-1].replace('.logs.tgz', '').replace('-justin', '@justin')
        paths[jobid] = path
    print(f'Got {len(paths)} paths from {len(lognames)} files')

  for ifile, jobid in enumerate(jobids):
    found_error = False
    found_input = False
    if args.use_web:
      fname, lines = get_webpage(ifile, jobid)
    else:
      fname, lines = get_justinlog(paths, jobid, ifile)
    
    if fname is None: continue
    
    if True:
      # print(flog.readlines())
      # lines = flog.readlines()
      ninput = 0
      this_expected = 0
      for l in lines:
        l = l.strip()
        if 'hadd Source' in l:
          # print(l)
          found_input = True
          all_inputs.append(l.split('/')[-1])
          ninput += 1
        if 'Got' in l and 'paths from' in l:
          this_expected = int(l.split()[1])
          expected_inputs += int(l.split()[1])
          print(colored(l, 'green'))
        if 'Error in <TFileMerger::AddFile>' in l or 'cannot open file' in l or 'error in header' in l:
          found_error = True
          print(colored(f'FOUND ERROR IN {jobid} KEEPING LOG', 'red'))
          print(l)

    # time.sleep(1)
    if ninput != this_expected:
      print(colored(f'Expected {this_expected}, got {ninput} in job {jobid}', 'yellow'))
    if (not found_error) and found_input:
      os.remove(fname)
    elif not found_input:
      print(colored(f'Did not find input for {f["name"]}', 'yellow'))


  print(f'{len(all_inputs)} Inputs')
  print(f'{len(set(all_inputs))} Unique Inputs')
  print(f'Expected {expected_inputs}')

  stamp = datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y%m%dT%H%M%S') 
  outname = f'{args.dataset.replace(":", "_")}_inputs_{stamp}.txt'
  print('Saving', outname)
  with open(outname, 'w') as f:
    f.writelines([l + '\n' for l in list(set(all_inputs))])

def check_duplicate_inputs(args):
  files = query(args)
  mc_ids = dict()
  for f in files:
    mc_id = f['name'].replace('.root', '').split('_')[-1]
    # print(mc_id)
    if mc_id not in mc_ids.keys():
      mc_ids[mc_id] = []
    else:
      print('Found duplicate', mc_id)
      # mc_ids[mc_id] += 1
    mc_ids[mc_id].append(f['fid'])

  file_states = dict()

  workflow = args.dataset.split('-')[-1].replace('w', '').replace('s1p1', '')
  print(workflow)
  cmd = ['justin', 'show-files', '--workflow-id', workflow]
  proc = subprocess.run(cmd, capture_output=True)
  for l in proc.stdout.decode('utf-8').split('\n'):
    splitted = l.split()
    if len(splitted) < 4: continue
    # print(splitted[2], splitted[-1])
    file_states[splitted[-1].split('-')[-1]] = splitted[2]
  

  for id, fids in mc_ids.items():
    count = len(fids)
    if count > 1:
      print(id, count)
      for fid in fids:
        print('\t', id, fid)
        children = mc.get_file(fid=fid, with_provenance=True)['children']
        for c in children:
          child = mc.get_file(fid=c['fid'], with_metadata=True)
          cmd = child['metadata']
          print('\t\t', child['name'], cmd['dune.workflow']['workflow_id'], cmd['dune.workflow']['job_id'])
    else:
      if file_states[id] != 'processed':
        print(f'Found file with MC ID {id} with state {file_states[id]}')


def check_jobsub_states(args):
  files = query(args)
  jobids_from_files = []
  for f in files:
    jobid = f['metadata']['dune.workflow']['job_id']
    print(jobid)
    jobids_from_files.append(jobid)

  workflow = args.dataset.split('-')[-1].replace('w', '').replace('s1p1', '')
  print(workflow)

  job_states = dict()

  cmd = ['justin', 'show-jobs', '--workflow-id', workflow]
  proc = subprocess.run(cmd, capture_output=True)
  for l in proc.stdout.decode('utf-8').split('\n'):
    splitted = l.split()
    if len(splitted) < 4: continue
    # print(splitted[0], splitted[3])
    job_states[splitted[0]] = splitted[3]
  
  for jobid in jobids_from_files:
    state = job_states[jobid]
    # print(state)
    if state != 'finished': print('Unfinished', jobid, state)
    else: print('Finished')
def crosscheck(args):
  # infiles = args.dataset.split(':')
  infiles = args.check_files
  inputs = []
  for fname in infiles:
    with open(fname, 'r') as f:
      inputs += [l.strip() for l in f.readlines()]

  print(f'{len(inputs)} inputs')
  print(f'{len(set(inputs))} unique inputs')

if __name__ == '__main__':
  parser = ap()
  parser.add_argument(
    'routine', type=str, default='merge',
    choices=[
      'merge', 'check', 'crosscheck', 'check_inputs',
      'check_parents', 'check_duplicate_inputs',
      'check_jobsub_states'
    ]
  )
  parser.add_argument('--dataset', type=str, required=True)
  parser.add_argument('--namespace', type=str, default=None)
  parser.add_argument('-o', type=str, default='inherit')
  parser.add_argument('--skip', type=int, default=0)
  parser.add_argument('--iter', type=int, default=None)
  parser.add_argument('--limit', type=int, default=1000)
  parser.add_argument('--run', type=int, default=1)
  parser.add_argument('--subrun', type=int, default=1)
  parser.add_argument('--dry-run', action='store_true')
  parser.add_argument('--skip_checksum', action='store_true')
  parser.add_argument('--iteration', type=int, default=2)
  parser.add_argument('--use_web', action='store_true')
  parser.add_argument('--check_files', type=str, nargs='+')
  args = parser.parse_args()

  if args.routine == 'merge':
    do_merge(args)
  elif args.routine == 'check':
    from termcolor import colored
    do_check(args)
  elif args.routine == 'crosscheck':
    crosscheck(args)
  elif args.routine == 'check_inputs':
    check_inputs(args)
  elif args.routine == 'check_parents':
    check_parents(args)
  elif args.routine == 'check_duplicate_inputs':
    check_duplicate_inputs(args)
  elif args.routine == 'check_jobsub_states':
    check_jobsub_states(args)
