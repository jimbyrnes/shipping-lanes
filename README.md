# shipping-lanes
Cargo ship streaming for streaming self-education

## AISStream coverage probe

A minimal probe that checks whether AISStream delivers usable AIS data for the Strait of Hormuz and Bab el-Mandeb before any streaming infrastructure is built.

```bash
.venv/bin/python -m pytest
.venv/bin/python -m aisprobe --duration 900
```

It needs `AISSTREAM_API_KEY` in the environment. Output goes to `data/`, which Git ignores. See [docs/coverage-probe.md](docs/coverage-probe.md).
