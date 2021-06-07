import os
from subprocess import check_output
from typing import Dict, List, Optional
import dataclasses
import pathlib
import csv

NUMBER_OF_KEYS = 91125
NUMBER_OF_QUERIES = 10 ** 6
# NUMBER_OF_QUERIES = 10 ** 7

"""stats shard_id=0 elapsed=0.036335127 served_own_gets=0 served_own_sets=101125 served_own_deletes=0 served_forwarded_gets=0 served_forwarded_sets=0 served_forwarded_deletes=0 forwarded_gets=0 forwarded_sets=0 forwarded_deletes=0"""


@dataclasses.dataclass
class Stats:
    shard_id: int
    elapsed: float
    served_own_gets: int
    served_own_sets: int
    served_own_deletes: int
    served_forwarded_gets: int
    served_forwarded_sets: int
    served_forwarded_deletes: int
    forwarded_gets: int
    forwarded_sets: int
    forwarded_deletes: int
    num_queries: int
    mean: float

    def throughput(self) -> float:
        total = 0
        total += self.served_own_gets
        total += self.served_own_sets - NUMBER_OF_KEYS  # exclude initial data loading
        total += self.served_own_deletes
        total += self.served_forwarded_gets
        total += self.served_forwarded_sets
        total += self.served_forwarded_deletes
        return total / self.elapsed

    @classmethod
    def from_string(cls, s: str, num_queries: int) -> "Stats":
        if s.startswith("stats"):
            s = s[6:]
        items = s.split()
        res = {}
        for item in items:
            k, v = item.split("=")
            if k in {"elapsed", "mean"}:
                v = float(v)
            else:
                v = int(v)
            res[k] = v
        return cls(num_queries=num_queries, **res)


def get_env(
    number_of_queries: int,
    remote_percentage: int,
    workload: str,
    write_percentage: Optional[int] = None,
) -> Dict:
    return {
        "RUST_BACKTRACE": "1",
        "NUM_QUERIES": str(number_of_queries),
        "REMOTE_PERCENTAGE": str(remote_percentage),
        "WRITE_PERCENTAGE": str(write_percentage or 0),
        "WORKLOAD": workload,
        "BENCHMARK": "1",
    }


def extend_env(env: Dict) -> Dict:
    res = os.environ.copy()
    res.update(**env)
    return res


def run(env: Dict) -> List[Stats]:
    cmd = "cargo run --package parsley --bin parsley --release"
    # cmd = "cargo flamegraph --bin parsley -o my_flamegraph.svg"
    # cmd = "sudo /home/dmitry/.cargo/bin/flamegraph -o my_flamegraph.svg target/release/parsley"
    print("cmd!!", cmd)
    import pprint

    pprint.pprint(env)
    result = check_output(cmd.split(), env=extend_env(env), text=True)
    for line in result.splitlines():
        print(line)
    return [
        Stats.from_string(line, num_queries=NUMBER_OF_QUERIES)
        for line in result.splitlines()
    ]


def total_throughput(stats: List[Stats]):
    total = 0
    for stat in stats:
        total += stat.throughput()
    return total


def check():
    stats = run(
        get_env(
            number_of_queries=NUMBER_OF_QUERIES,
            remote_percentage=30,
            workload="ReadLocal100",
        )
    )
    print(stats)


@dataclasses.dataclass
class Workload:
    name: str
    remote_percentage: int
    write_percentage: Optional[int] = None
    number_of_queries: int = NUMBER_OF_QUERIES


def run_workload(workload: Workload) -> List[Stats]:
    env = get_env(
        number_of_queries=workload.number_of_queries,
        remote_percentage=workload.remote_percentage,
        write_percentage=workload.write_percentage,
        workload=workload.name,
    )
    stats = run(env)
    return stats


def run_workloads(log_file: pathlib.Path, workloads: List[Workload]):
    stats = []
    with log_file.open("w") as f:
        writer = csv.DictWriter(
            f,
            [
                "shard",
                "remote_percentage",
                "write_percentage",
                "total_throughput",
                "mean",
            ],
        )
        writer.writeheader()
        for workload in workloads:
            workload_stats = run_workload(workload)
            stats.append(workload_stats)
            writer.writerow(
                {
                    "shard": "",
                    "remote_percentage": workload.remote_percentage,
                    "write_percentage": workload.write_percentage,
                    "total_throughput": total_throughput(workload_stats),
                    "mean": sum(stat.mean for stat in workload_stats)
                    / len(workload_stats),
                }
            )
        return stats


def bench_1(log_file: pathlib.Path):
    workloads = []
    for remote_percentage in range(0, 55, 5):
        workloads.append(
            Workload(
                name="ReadLocalRemote",
                remote_percentage=remote_percentage,
            )
        )
    run_workloads(log_file, workloads)


def bench_2(log_file: pathlib.Path):
    workloads = []
    for remote_percentage in range(0, 55, 5):
        workloads.append(
            Workload(
                name="WriteLocalRemote",
                remote_percentage=remote_percentage,
            )
        )
    run_workloads(log_file, workloads)


def run_workload_stats_per_shard(
    log_file: pathlib.Path, workload: Workload
) -> List[Stats]:
    stats = run_workload(workload)
    with log_file.open("w") as f:
        writer = csv.DictWriter(
            f,
            [
                "shard",
                "remote_percentage",
                "write_percentage",
                "total_throughput",
                "mean",
            ],
        )
        writer.writeheader()
        for stat in sorted(stats, key=lambda stat: stat.shard_id):
            writer.writerow(
                {
                    "shard": stat.shard_id,
                    "remote_percentage": workload.remote_percentage,
                    "write_percentage": workload.write_percentage,
                    "total_throughput": stat.throughput(),
                    "mean": stat.mean,
                }
            )
        return stats


def bench_3(log_file: pathlib.Path):
    workload = Workload(
        name="StorageWorkloadA", remote_percentage=0, write_percentage=30
    )
    run_workload_stats_per_shard(log_file=log_file, workload=workload)


def bench_4(log_file: pathlib.Path):
    workload = Workload(
        name="StorageWorkloadB", remote_percentage=0, write_percentage=70
    )
    run_workload_stats_per_shard(log_file=log_file, workload=workload)


def bench_5(log_file: pathlib.Path):
    # 30% for shards 0 and 1
    workload = Workload(
        name="StorageWorkloadC", remote_percentage=0, write_percentage=30
    )
    run_workload_stats_per_shard(log_file=log_file, workload=workload)


def bench_6(log_file: pathlib.Path):
    workload = Workload(
        name="PMemStorageWorkload", remote_percentage=0, write_percentage=30
    )
    run_workload_stats_per_shard(log_file=log_file, workload=workload)


def bench_7(log_file: pathlib.Path):
    # the same as bench 6 but without persist
    bench_6(log_file)
    pass


def main():
    log_dir = pathlib.Path(__file__).parent / "bench_logs"
    benches = [bench_1, bench_2, bench_3, bench_4, bench_5, bench_6, bench_7]
    for i, bench in enumerate(benches):
        log = log_dir / f"bench_{i+1}.log"
        bench(log)


if __name__ == "__main__":
    main()
