from metacat.webapi import MetaCatClient
mc = MetaCatClient()

from rucio.client.replicaclient import ReplicaClient
rc = ReplicaClient(account='justinreadonly')

from argparse import ArgumentParser as ap
#from datetime import datetime
import datetime

import subprocess, os

import json

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



if __name__ == '__main__':
  parser = ap()
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
  args = parser.parse_args()


  to_skip = args.skip if args.iter is None else args.iter*args.limit

  query = (
        f'files from {args.dataset} where dune.output_status=confirmed'
        f' ordered skip {to_skip} limit {args.limit}'
  )
  print(f'Querying {args.dataset} for {args.limit} files')
  print(f'Query: {query}')
  files = mc.query(query, with_metadata=True)

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
  for rep in reps:
      #print(rep)
      if len(list(rep['pfns'].keys())) == 0:
          print(f'\tWarning: skipping file with no pfn {rep["scope"]}:{rep["name"]}')
          #if args.save_no_rse:
          #    with open(f'no_rse_{r}_{stamp}.txt', 'a') as f_no_rse:
          #        f_no_rse.write(f'{rep["scope"]}:{rep["name"]}\n')
          continue
      paths.append(list(rep['pfns'].keys())[0])
  print(f'Got {len(paths)} paths from {len(names)} files')

  cmd = ['hadd', output_name] + paths
  print(cmd)

  if not args.dry_run:
    subprocess.run(cmd)

    results = finish_metadata(
      args,
      output_name,
      {'metadata':output_metadata}
    )

    json_object = json.dumps(results, indent=2)
    with open(f'{output_name}.json', 'w') as fjson:
      fjson.write(json_object)

    
