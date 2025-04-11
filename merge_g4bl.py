from metacat.webapi import MetaCatClient
mc = MetaCatClient()

from rucio.client.replicaclient import ReplicaClient
rc = ReplicaClient(account='justinreadonly')

from argparse import ArgumentParser as ap
import time

import subprocess

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('--dataset', type=str, required=True)
  parser.add_argument('-o', type=str, required=True)
  parser.add_argument('--skip', type=int, default=0)
  parser.add_argument('--limit', type=int, default=1000)
  parser.add_argument('--run', type=int, default=1)
  parser.add_argument('--subrun', type=int, default=1)
  parser.add_argument('--dry-run', action='store_true')
  args = parser.parse_args()

  query = (
        f'files from {args.dataset} where dune.output_status=confirmed'
        f' ordered skip {args.skip} limit {args.limit}'
  )
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

  output_metadata = {}

  ##TODO -- run and subrun

  print('Getting names and metadata')
  names = []
  for f in files:
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
  print(output_metadata)

  print('Getting paths from rucio')
  stamp = int(time.time()*100)

  #reps = rc.list_replicas(names, '''rse_expression='|'.join(args.rses)''')
  reps = rc.list_replicas(names)
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

  cmd = ['hadd', args.o] + paths
  print(cmd)

  if not args.dry_run:
    subprocess.run(cmd)
