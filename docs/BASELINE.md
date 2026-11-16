| Date | Command | Python | Machine | items/s | max lag |
|---|---|---|---|---|---|
| 2026-11-11 | `make baseline` (1000 dev, 24h, null sink) | 3.13 | MacBook Pro | 123,200 items/s | 0.000s |


Make serve:
Run 1: http sink: 230699 items, 462 batches, 462 requests, 230699 accepted, 0 duplicate, 0 rejected, 0 retries, 0 dropped, 20,145 items/s, request p50 18.0 ms, p99 106.6 ms
done: 230699 items, 462 batches, 11.46s wall, 20,139 items/s, max lag 0.000s
Run 2: http sink: 230699 items, 462 batches, 462 requests, 230699 accepted, 0 duplicate, 0 rejected, 0 retries, 0 dropped, 20,993 items/s, request p50 17.1 ms, p99 57.6 ms
done: 230699 items, 462 batches, 10.99s wall, 20,983 items/s, max lag 0.000s

Make serve KV never:
Run 1: 230699 items, 462 batches, 462 requests, 230699 accepted, 0 duplicate, 0 rejected, 0 retries, 0 dropped, 22,107 items/s, request p50 16.6 ms, p99 66.3 ms
done: 230699 items, 462 batches, 10.44s wall, 22,096 items/s, max lag 0.000s
Run 2: http sink: 230699 items, 462 batches, 462 requests, 230699 accepted, 0 duplicate, 0 rejected, 0 retries, 0 dropped, 21,711 items/s, request p50 17.2 ms, p99 46.4 ms
done: 230699 items, 462 batches, 10.63s wall, 21,703 items/s, max lag 0.000s

Make serve Postgres commit off:
Run 1: http sink: 230699 items, 462 batches, 462 requests, 230699 accepted, 0 duplicate, 0 rejected, 0 retries, 0 dropped, 21,369 items/s, request p50 17.1 ms, p99 77.2 ms
done: 230699 items, 462 batches, 10.80s wall, 21,359 items/s, max lag 0.000s
Run 2: http sink: 230699 items, 462 batches, 462 requests, 230699 accepted, 0 duplicate, 0 rejected, 0 retries, 0 dropped, 22,769 items/s, request p50 16.1 ms, p99 63.5 ms
done: 230699 items, 462 batches, 10.14s wall, 22,760 items/s, max lag 0.000s

Config	items/s	request p50 ms	request p99 ms	notes
A defaults (KV interval, commit on)	20.1k to 21.0k	17.1 to 18.0	57.6 to 106.6	230,699 events, 462 batches of 500
B KV never	21.7k to 22.1k	16.6 to 17.2	46.4 to 66.3	
C synchronous_commit off	21.4k to 22.8k	16.1 to 17.1	63.5 to 77.2	Postgres in Docker Desktop on macOS

Removing the KV fsync or the Postgres commit fsync each gave about 6 to 7% throughput, close to the 4% spread between identical runs. The commit fsync costs about 1 ms of a 17 ms b
During a serial run the server process peaked at about 35% of one core, so it is not CPU-bound. Per 500-event batch (about 24 ms cycle): about 7 ms client generation, at most about 8 ms server CPU, about 9 ms waiting on Postgres.
One serial client measures pipeline latency, not server capacity. Capacity needs parallel clients, which needs a --device-offset flag so concurrent simulators use distinct device ids.