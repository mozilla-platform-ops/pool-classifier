# Feature request: bounded plaintext tails for Taskcluster log artifacts

## Summary

Taskcluster should provide a supported way to retrieve the final bytes of a
task log as **plaintext**, without requiring clients to download and
decompress the entire artifact.

This is needed by pool-classifier and similar consumers that classify terminal
task failures from a bounded amount of log text. Test harnesses commonly emit
their failure summary at the end of `public/logs/live_backing.log`.

## Current limitation

Task log artifacts are stored gzip-compressed with `Content-Encoding: gzip`.
By default, the artifact host performs decompressive transcoding. A HTTP
`Range` request against that plaintext response is silently ignored and the
entire decompressed object is returned.

Requesting `Accept-Encoding: gzip` avoids transcoding and permits a range over
the compressed object. That does not solve plaintext tail retrieval: a suffix
of a gzip stream cannot be independently decompressed.

Consequently, a bounded client has only two undesirable choices:

1. Download and decompress the complete artifact, which has unbounded
   plaintext size and processing cost.
2. Stop after a fixed number of decompressed bytes, which captures a prefix,
   not the actual end of the log.

Pool-classifier works around this for small compressed artifacts by downloading
the complete gzip stream, decoding it locally, and retaining only a fixed
plaintext head and true tail. It must still reject larger or slow artifacts to
protect the service; a server-side plaintext-tail capability would remove that
gap without requiring the client to read and inflate the complete log.

## Concrete example

Task `Pv2HuZgyQUaD5LyqRsN3dQ` failed on 2026-08-18. Its terminal summary was
in the final lines:

```text
22:10:52  WARNING - Got 21 unexpected statuses
22:10:52  WARNING - Got 21 unexpected crashes
```

The artifact was 2,403,960 bytes compressed and 34,859,497 bytes after
decompression. Before the gzip-stream fallback, the bounded client stopped
after 5,242,880 plaintext bytes, around 21:50, and therefore could not
classify the failure from rules that match either summary line.

## Requested capability

Expose a Taskcluster-supported artifact operation that returns a bounded,
uncompressed suffix of an artifact. For example, an artifact endpoint or
parameter with these semantics:

```text
GET .../artifacts/public/logs/live_backing.log?tail_bytes=65536
```

The response should:

- return at most the requested number of final *uncompressed* bytes;
- be independently readable as plaintext (not a partial gzip stream);
- return `206 Partial Content` and an appropriate `Content-Range` describing
  the plaintext representation, when applicable;
- avoid transferring or charging clients for the entire decompressed artifact;
- preserve normal authorization and artifact-retention behavior.

An equivalent design is a small, separately addressable terminal-log artifact
written by the worker/runtime, such as
`public/logs/live_backing.tail.log`. A server-side log index or a seekable
compression format would also satisfy the requirement.

## Why this matters

Failure summaries and mozharness result counts are typically emitted at task
shutdown. Efficient true-tail retrieval would allow operational consumers to
classify large, noisy logs reliably while maintaining strict network, memory,
and CPU bounds.
