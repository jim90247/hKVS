# Combined

## Configuration

### HERD

#### Thread

Change the following items:

- `herd_workers` in [`scripts/combined/launch_server.sh`](../../scripts/combined/launch_server.sh#L22)
- `NUM_WORKERS` and `NUM_CLIENTS` in [`src/herd/main.h`](../herd/main.h#L33) and [`src/combined/herd_main.h`](herd_main.h#L38)
- `--herd_threads X` in [`scripts/combined/launch_client.sh`](../../scripts/combined/launch_client.sh#L32), where `X` is the number of HERD threads at that client process.

#### Value size

Change the following items:

- `HERD_VALUE_SIZE` in [`src/herd/main.h`](../herd/main.h#L23) and [`src/combined/herd_main.h`](herd_main.h#L25).
- Increase `MICA_OBJ_SIZE` in [`src/mica/mica.h`](../mica/mica.h#L28) if needed. See `MICA_MAX_VALUE` in the same file for the maximum supported value size under different settings.
- Decrease `HERD_NUM_KEYS` in [`src/herd/main.h`](../herd/main.h#L22) and [`src/combined/herd_main.h`](herd_main.h#L24) if needed.
  - For example, when `MICA_OBJ_SIZE` is 1024, we may change this value to `262144`.
- Increase `RR_SIZE` in [`src/herd/main.h`](../herd/main.h#L46) and [`src/combined/herd_main.h`](herd_main.h#L53) if needed. Note that changing this value might need more hugepages.
  - For example, using 24 HERD workers at server, 72 HERD client threads and `MICA_OBJ_SIZE` being 1024, we may change this value to `128 << 20` and use 65536 2MB huge pages.
