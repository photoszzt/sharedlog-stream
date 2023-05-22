import argparse
import json
import os


def load_stats(stats_dir, num):
    stats = {}
    for i in range(num):
        with open(f'./{stats_dir}/q8JoinStream-{i}.json') as f:
            q8_st = json.load(f)
            stats[i] = q8_st
    return stats


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--stats_dir', type=str)
    parser.add_argument('--start_ts', type=int)
    args = parser.parse_args()
    st_af = load_stats(os.path.join(args.stats_dir, "afterScale"), 4)
    st_bf = load_stats(os.path.join(args.stats_dir, "beforeScale"), 2)

    for j in range(2):
        bf_ts = [i-args.start_ts for i in st_bf[j]['EventTs']]
        af_ts = [i-args.start_ts for i in st_af[j]['EventTs']]
        print(f'worker {j} bf {bf_ts}')
        print(f'worker {j} af {af_ts}')
    #for i in range(2):
    #    print(f'worker {i} bf {[int(i) - args.start_ts for i in st_bf[i]['EventTs']]}')
    #    print(f'worker {i} af {[int(i) - args.start_ts for i in st_af[i]['EventTs']]}')


if __name__ == '__main__':
    main()
