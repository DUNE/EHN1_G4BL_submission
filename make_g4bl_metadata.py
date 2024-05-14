from argparse import ArgumentParser as ap
import json

def check_momentum(args):
  if args.momentum < 0.: 
    raise ValueError('Need to provide momentum above 0. Instead provided', args.momentum)

def checks(args):
  check_momentum(args)

if __name__ == '__main__':
  parser = ap()
  parser.add_argument('--bl', choices=['H2', 'H4', 'h2', 'h4'], default='H4',
                      type=str, help='Which beamline? upper/lowercase agnostic')
  parser.add_argument('--polarity', choices=['pos', 'neg'], type=str,
                      default='pos', help='Which polarity?')
  parsre.add_argument('--momentum', type=float, help='Which momentum?',
                      default=1.)
  args = parser.parse_args()

  checks(args)

  run_type = 'ehn1-beam-np0' + args.bl.lower().replace('h', '')

  base_meta = {
    'core.data_tier':'root-tuple-virtual', #Becomes root-tuple when merged
    'core.file_format': 'root', #TODO -- double check this isn't rootntuple
    'core.run_type':run_type,
    'core.file_type':'mc', 
    'dune.':
    'beam.momentum': args.momentum,
    'beam.polarity': (1 if args.polarity == 'pos' else -1),
  }
