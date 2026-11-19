# MPI rank log collector

Python 3.9+; no external packages required.

## Execute Open MPI and assemble output by rank

```bash
python3 mpi_rank_logs.py -o combined.log --stats run -- \
  mpirun --output tag -np 4 ./my_program
```

For older Open MPI:

```bash
python3 mpi_rank_logs.py -o combined.log run -- \
  mpirun --tag-output -np 4 ./my_program
```

## Read an existing combined tagged log

```bash
mpirun --tag-output -np 4 ./my_program > raw.log 2>&1
python3 mpi_rank_logs.py --stats -o combined.log parse raw.log
# or
cat raw.log | python3 mpi_rank_logs.py parse
```

Options must go before `run` or `parse`: `-o FILE`, `--stats`, `--hide-untagged`.

The tool accepts the Open MPI `[job,rank]<stdout>:` and `[rank]<stdout>:` formats,
plus full-name / detailed tags. It retains per-rank arrival order and displays
untagged launch diagnostics in a separate section. stdout and stderr order
between ranks is not globally synchronized. It does not reconstruct messages
that were split across ranks or stripped of their original tags.

For very large logs it spools individual rank data to temporary files rather
than accumulating the full log in memory. Temporary files are removed on normal
completion. Ensure there is sufficient temporary disk space (controlled by
`TMPDIR`).

## Test

```bash
python3 -m unittest -v test_mpi_rank_logs.py
```
