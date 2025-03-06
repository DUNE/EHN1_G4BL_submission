from argparse import ArgumentParser as ap
import os
import json

def check_momentum(args):
  if args.momentum < 0.: 
    raise ValueError('Need to provide momentum above 0. Instead provided', args.momentum)

def checks(args):
  check_momentum(args)

def get_full_file(args):
  return (args.path + "/" + args.name if args.path is not None else args.name)

def get_size(args):
  filename = args.path + "/" + args.name if args.path is not None else args.name
  return os.path.getsize(get_full_file(args))
  #return os.path.getsize(filename)
  
def get_adler32(args):
  import subprocess
  proc = subprocess.run(
    ['xrdadler32', get_full_file(args)],
    capture_output=True,
  )

  if proc.returncode != 0:
    raise Exception('Error in checksum creation' + proc.stderr.decode('utf-8'))

  return proc.stdout.decode('utf-8').split()[0]

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('--bl', choices=['H2', 'H4', 'h2', 'h4'], default='H4',
                      type=str, help='Which beamline? upper/lowercase agnostic')
  parser.add_argument('--polarity', choices=['pos', 'neg'], type=str,
                      default='pos', help='Which polarity?')
  parser.add_argument('--momentum', type=float, help='Which momentum?',
                      default=1.)
  parser.add_argument('--run', type=int, help='Run number', required=True)
  parser.add_argument('--subrun', type=int, help='Subrun number', required=True)
  parser.add_argument('--name', type=str, help='File name', required=True)
  parser.add_argument('--namespace', type=str, help='File namespace/scope', required=True)
  parser.add_argument('--path', default='', help='Optional path to file directory')
  parser.add_argument('--checksum', action='store_true', help='Add adler32 checksum')
  parser.add_argument('--add_status', action='store_true', help='Add core.file_content_status=good')
  args = parser.parse_args()

  checks(args)

  run_type = 'ehn1-beam-np0' + args.bl.lower().replace('h', '')


 

  base_meta = {
    'name':args.name,
    'namespace':args.namespace,
    'size':get_size(args),
    'metadata': {
      'core.group': 'dune',
      'core.data_tier':'root-tuple', #Becomes root-tuple when merged
      'core.file_format': 'root', #TODO -- double check this isn't rootntuple
      'core.run_type':run_type,
      'core.file_type':'mc', 
      'core.data_stream':'g4beamline',
      'beam.momentum': args.momentum,
      'beam.polarity': (1 if args.polarity == 'pos' else -1),
      'core.runs': [args.run],
      'core.runs_subruns': [int(args.run*1e5 + args.subrun)],
      'retention.class':'physics',
      'retention.status':'active',
    }
  }

  if args.checksum:
    base_meta['checksums'] = {
      'adler32':get_adler32(args),
    }
  if args.add_status:
    base_meta['metadata']['core.file_content_status'] = 'good'

  with open(args.name + '.json', 'w') as outfile:
    json_object = json.dumps(base_meta, indent=2)
    outfile.write(json_object)

