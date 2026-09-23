# Local operational boundary

The service binds to loopback and is a single-user experimental tool. It is not
a public hosted API or production rollout certification system. Browser requests
require same-origin plus a custom mutation header; trusted host checks prevent
arbitrary hostnames. No external analytics, hosted fonts or telemetry are used.

Endpoint/hook registration comes from operator-owned startup configuration.
Exact origins are allowlisted. HTTPS verification stays enabled. Redirects and
environment proxy inheritance are disabled. Private/loopback destinations need
explicit host exceptions; link-local metadata, multicast and unspecified IPs are
always rejected. DNS is resolved and validated, and the connection is pinned to
that address while preserving the original TLS server name and Host header.
No arbitrary URL fetching is available through experiment or MCP arguments.

Provider credentials are resolved only in the backend from the configured
environment name. They are not exported, printed, embedded in browser assets or
stored in localStorage. HTTP error bodies are not copied into evidence. Relevant
provider request IDs are retained. Request IDs themselves may be sensitive and
are not automatically anonymized.

Every run declares request, token, duration, concurrency and resource bounds.
The scheduler reserves conservative prompt-byte-plus-framing and maximum-output
tokens for all offered requests before traffic starts. The provider must honor
its output constraint; this is not a guarantee about an adversarial provider's
billing. No dollar ceiling is claimed without dated price inputs. No automatic
generation retry is performed. Only one live/managed run may use a service's
endpoint envelope concurrently. Repeated separately authorized runs incur their
own bounded cost.

Stop prevents new dispatch and cancels in-flight HTTP work. Offered rows remain
in evidence. Provider cancellation may not cancel all server-side billing; its
unknown remainder is disclosed. Deployment cancellation attempts bounded
rollback; an uncertain outcome retains the lease for operator reconciliation.

Deployment approval requires `X-Operator-Token`, separate from provider keys.
The exact immutable plan hash binds target, configurations, generations, resource
budget and expiry. Current state is revalidated. The hook requires a separate
bearer token, typed operation names and durable idempotency. No Docker socket is
mounted into the analysis UI/API.

Imports allow only the fixed evidence filenames, reject links, nested/traversal
paths, duplicates, unexpected files and archives exceeding 32 MiB uncompressed.
Reports escape imported text and use no scripts/remote assets. Schema versions,
clock order, request IDs and claims are checked independently. Checksums provide
integrity relative to the manifest, not authenticity.

Default evidence stores metadata, lengths, structural groups and validation
outcomes. Prompt/response text is omitted, including for synthetic prompts.
Metadata-only evidence cannot independently reassess semantic answers. An
operator may explicitly import a workload with text into their own local script;
that input has a separate retention responsibility. Hashing is not anonymization.

Data lives under the selected `--data-dir`. There is no automatic expiry or
outbound backup. To delete an experiment, stop its owning service, export any
evidence you need, then remove only that owned data directory. Never delete an
active hook database/lease to bypass unresolved mutation state. Temporary test
services are stopped by their test fixtures; GPU containers are cleaned up only
after ownership, in-flight traffic and retained evidence are checked.
