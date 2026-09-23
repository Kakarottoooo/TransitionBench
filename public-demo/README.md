# Public recorded-case demo

This directory mirrors the published static demo. It displays the sealed September 22, 2026 result file; choosing a window does not run a new forecast or GPU experiment. It has no upload form, application backend, API credentials or analytics code.

To preview from the repository root:

```sh
python -m http.server 8781 --bind 127.0.0.1 --directory public-demo
```

Open http://127.0.0.1:8781/. `result.json` is byte-identical to `web/public/examples/independent-test/result.json`. Evidence downloads and the verifier link to the immutable v0.4.4 release. The Archivo font retains its bundled OFL license.

The full local application, its measured-evidence import, review and observation APIs remain separate. Public source updates do not modify the existing release wheel.
