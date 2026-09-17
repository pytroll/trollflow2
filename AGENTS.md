# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common commands

Run the tests (from the repo root):

```
pytest trollflow2/tests
```

Single file / single test:

```
pytest trollflow2/tests/test_launcher.py
pytest trollflow2/tests/test_trollflow2.py::TestSaveDatasets::test_save_datasets
```

Lint (ruff is the sole linter, configured in `pyproject.toml`; line length 120):

```
ruff check .
```

Pre-commit hooks (ruff + trailing-whitespace + end-of-file-fixer + isort) run via `pre-commit run --all-files`.

The CI environment is a conda env built from `continuous_integration/environment.yaml`; install for editable dev with `pip install --no-deps -e .` after the conda env is active. Python 3.12+ is required (declared in `pyproject.toml`), CI tests 3.12/3.13/3.14.

## Entry points

Two console scripts are declared in `pyproject.toml`:

- `satpy_launcher.py` → `trollflow2.satpy_launcher:main` → `trollflow2.launcher.launch`. This is the daemon: it listens on Posttroll topics and processes each incoming file/dataset/collection message.
- `satpy_cli` → `trollflow2.cli:cli`. One-shot mode that runs the product list against a fixed set of input files supplied on the command line (no Posttroll listener). Useful for reprocessing and for the dask profiler flags.

Both funnel into the same `process_files` → `file_list_to_jobs` → `process_jobs` pipeline inside `trollflow2/launcher.py`.

## Architecture

Trollflow2 is a **YAML-configured pipeline runner around Satpy**. The user writes a *product list* (see `examples/pl.yaml`); the launcher turns each incoming Posttroll message into one or more *jobs* and pushes each job through an ordered list of *worker* callables (the "plugins").

### The run loop (`trollflow2/launcher.py`)

1. `Runner.run()` either iterates the Posttroll listener (`generate_messages`) or replays a single test message.
2. For every message, a **new subprocess** is spawned via `create_logged_process` (multiprocessing `spawn` context) that runs `queue_logged_process` → `process` → `process_files`. Threaded mode (`--threaded` / `-t`) exists for tests and short jobs; production uses subprocesses to prevent memory buildup between scenes.
3. `process_files` reads and expands the YAML product list with `UnsafeLoader` (custom `!tuple` constructor + `expand()` to deep-copy YAML aliases so per-area mutations don't leak), optionally creates a dask distributed client from `dask_distributed.class`/`settings`, then calls `file_list_to_jobs` and `process_jobs`.
4. `file_list_to_jobs` fans a message out **by area priority** (each `areas.<area>.priority` value, default 999). Areas that share a priority are grouped into one job dict; lower numbers run first. Each priority group produces a *job dict* containing `input_filenames`, `input_mda`, and a scoped `product_list` (only that priority's areas).
5. `process_jobs` walks the `workers:` list and calls each worker's `fun(job, **kwargs)` in order, threading the same `job` dict through the whole chain. If a worker raises `AbortProcessing`, the remaining workers for that priority are skipped but processing continues with the next priority. A per-worker `timeout` (seconds) is enforced with `SIGALRM`.
6. After the subprocess exits, `check_results` drains the `produced_files` multiprocessing Queue and checks each URI (local file via `os.path.getsize`, `s3://` via `trollflow2.plugins.s3.check_s3_file`). Empty or missing files become errors.

### Worker (plugin) contract (`trollflow2/plugins/__init__.py`)

- Workers are either bare functions `fun(job, **kwargs)` **or** classes with `__call__(self, job)`. Class-based plugins that need constructor arguments must implement `__setstate__` because they are instantiated by YAML via `!!python/object:...`. If a plugin has a `stop()` method, `process_files` calls it during cleanup.
- Workers communicate exclusively via mutation of the `job` dict. Typical keys added along the way: `scene` (created by `create_scene`), `resampled_scenes` (by `resample`, keyed by area name), `processing_priority`, `produced_files`.
- **Ordering matters.** A reasonable default chain is `create_scene → load_composites → resample → save_datasets → FilePublisher`; auxiliary filters (`covers`, `check_metadata`, `metadata_alias`, `sza_check`, `check_sunlight_coverage`, `check_valid_data_fraction`) are inserted where they can short-circuit the earliest, and `add_overviews` runs between `save_datasets` and `FilePublisher`.
- Most plugin configuration is read from the product list through `_get_plugin_conf` / `get_config_value` in `dict_tools.py`, which does a **hierarchical lookup**: it walks up the dpath from the most specific level (product → area → product_list root → `/common/`) and returns the first hit. This is why the same option (e.g. `resampler`, `output_dir`, `fname_pattern`) can be set at any level and be overridden further down.
- `plist_iter(product_list, level=…)` is the standard iterator over the product list; `level='area'` yields once per area, `level='product'` once per (area, product), and the default yields once per (area, product, format). Each yield is `(flattened_config, local_config)` where `flattened_config` has inherited all parent-level keys.

### Save + publish

- `save_datasets` collects delayed Satpy write results, optionally writes through `staging_zone`/`use_tmp_file` (via the `prepared_filename` context and the `renamed_files` post-rename context), computes them together with `satpy.writers.compute_writer_results`, and pushes the final filenames onto `job['produced_files']` (the multiprocessing Queue). `call_on_done` callbacks (`callback_log`, `callback_move`, `callback_close`) can be wired for post-write actions; `early_moving: True` moves files as each writer finishes rather than after the whole scene.
- `FilePublisher` (class-based worker) publishes a Posttroll `file` message per produced file, composing the topic from `publish_topic` using `trollsift.compose`, and additionally emits `dispatch` messages for each entry in `formats[*].dispatch`.

### Logging

Trollflow2 uses **queued logging** across subprocesses (see `trollflow2/logging.py`). The parent installs a `QueueListener`; each subprocess installs a `QueueHandler` writing to the shared `multiprocessing.Manager` queue created by the `get_manager()` singleton in `trollflow2/__init__.py`. The `queued_logging` decorator on `queue_logged_process` pops `log_queue`/`log_config` from kwargs before calling `process`. `SIGUSR1` sent to a subprocess dumps tracebacks of all threads (see `print_traces`).

### Subprocess signalling

- Send `SIGTERM` to the launcher to stop accepting new messages and exit after the current job completes.
- Send `SIGUSR1` to a running worker subprocess to dump all thread tracebacks to stderr.

## Testing notes

- Tests live in `trollflow2/tests/`. `test_trollflow2.py` covers plugins, `test_launcher.py` covers the run loop and YAML handling, `test_logging.py` covers the queued logging setup, `test_s3_plugins.py` covers the S3 helpers, `test_cli.py` covers the one-shot CLI.
- Ruff is configured to allow `assert` in tests (`per-file-ignores` in `pyproject.toml`).
- The test env pulls `pytroll-schedule`, `s3fs`, `netcdf4`, `bokeh`, `rasterio` in addition to the runtime deps — several plugins import these lazily and their tests will skip / error without them.
