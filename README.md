# snaport

**snaport** downloads AWS EC2 EBS snapshots (standalone, or every volume of an
AMI) using the official [EBS Direct APIs](https://docs.aws.amazon.com/ebs/latest/essbs/ebs-direct-api.html)
— no volumes, no instances, no third-party snapshot tools — and stores them as
**NTFS sparse raw images** on your Windows machine: only allocated blocks
consume disk space. Every block is checksum-verified against the value AWS
returns, the whole image is hashed, and the result is compressed with zstd and
restore-tested before the raw image is discarded.

Windows-first; the code paths are portable (Linux/macOS sparse files work too,
just not the primary target).

## Why

- **No EBS volume or EC2 instance needed** — the EBS Direct APIs
  (`ListSnapshotBlocks` / `GetSnapshotBlock`) read snapshot data directly.
- **Storage-efficient** — a 500 GiB snapshot with 40 GiB of allocated blocks
  needs roughly 40 GiB during download and ~20-30 GiB after compression,
  never 500 GiB (see [Disk usage](#disk-usage) below).
- **Minimal IAM** — two `ebs:*` actions (+ `ec2:DescribeImages` for AMIs,
  `kms:Decrypt` for encrypted snapshots).
- **Integrates into your own backup workflow** — single static binary,
  JSON manifests, scriptable.

## Install

Requires Go 1.22+:

```
go build -o snaport.exe ./cmd/snaport
```

or with a version stamp:

```
go build -ldflags "-X snaport/internal/cli.Version=0.1.0" -o snaport.exe ./cmd/snaport
```

## Usage

```
snaport download snap-0123456789abcdef0                 # one snapshot -> ./snap-....img(.zst)
snaport download ami-0123456789abcdef0 -o D:\backups    # all EBS volumes -> D:\backups\ami-...\*.img.zst
snaport download snap-0123456789abcdef0 --dry-run       # plan only: sizes, blocks, cost, disk headroom
snaport verify D:\backups\ami-0123456789abcdef0         # re-verify a finished download
snaport selftest                                         # offline end-to-end test, no AWS needed
```

Credentials and region resolve through the standard AWS chain (env vars,
`~/.aws/credentials` profiles, instance/container roles); override with
`--profile` / `--region`.

### Key flags (`snaport download`)

| Flag | Default | Meaning |
|---|---|---|
| `-o, --output` | `./<id>.img` or `./<ami-id>/` | output file (snapshot) or directory (AMI) |
| `--concurrency` | 32 | parallel `GetSnapshotBlock` requests (auto-reduced on throttling) |
| `--zstd-level` | 6 | compression level 1-19 |
| `--keep-raw` | off | keep the sparse raw image next to the `.zst` after verified compression |
| `--no-compress` | off | keep only the raw sparse image |
| `--ntfs-compress` | off | also apply transparent NTFS compression to the raw image |
| `--paranoid-resume` | off | spot-check already-downloaded blocks against checksums when resuming |
| `--force` | off | discard manifest/journal/image and restart |
| `--dry-run` | off | list blocks; estimate sizes, AWS cost and free space; transfer nothing |
| `--root-only` | off | AMI mode: only the root volume |
| `--wait` | off | wait up to e.g. `--wait 15m` for pending snapshots to complete before downloading |
| `--state-dir` | alongside output | where resume journals live |
| `--egress-per-gb` | 0.09 | USD/GB internet egress rate used in `--dry-run` cost estimates (0 = ignore egress, e.g. inside AWS) |

## How it works

1. `ListSnapshotBlocks` pages through the snapshot's allocated blocks
   (index + short-lived block token + token expiry time).
2. A worker pool calls `GetSnapshotBlock` per block, verifying the
   base64 SHA-256 the API returns against the downloaded bytes before
   accepting them.
3. Each verified block is written at `blockIndex × blockSize` in a
   **sparse** image file (marked via `FSCTL_SET_SPARSE` on NTFS).
   Unallocated regions and allocated-but-zero blocks are never written —
   they stay holes and read back as zeros, exactly matching EBS semantics.
   The file's logical length is set to exactly `volumeGiB × 1 GiB`.
4. A verification pass re-reads the image: every block's checksum is
   re-checked and the SHA-256 of the whole logical image is recorded in
   the manifest.
5. The image is stream-compressed to `.img.zst`; the archive is then
   **restore-tested** — decoded end-to-end and its decoded hash compared
   with the image hash. Only then is the raw image deleted (unless
   `--keep-raw`).
6. Artifacts per snapshot: `foo.img.zst` + `foo.manifest.json` (geometry,
   per-block checksums, whole-image hash, compression metadata).

AMI mode resolves volumes via `DescribeImages` and produces one
image/manifest per EBS-backed device plus a top-level `ami-manifest.json`.

## Disk usage

For a 500 GiB snapshot with 40 GiB of allocated blocks:

| Phase | On disk |
|---|---|
| Downloading | ~40 GiB (sparse raw image; holes are free) |
| Compress + verify peak | ~60-70 GiB (raw + `.zst`) |
| Final (default) | just the `.zst` (~20-30 GiB) |

`--dry-run` prints the allocated-block estimate, the projected AWS cost
and your free space before anything is transferred.

## Browsing a downloaded image

Downloaded images are raw disk images (GPT/MBR partitioned), so Windows
needs help to look inside:

- **Linux filesystems (ext4/XFS - typical EC2 root volumes)** - use WSL2:

  ```
  tools\mount-image.cmd C:\path\to\snap-xxxx.img
  ```

  It attaches a loop device, mounts partition 1 **read-only** (with an
  XFS `norecovery` fallback for volumes snapshotted while mounted) and
  prints the Explorer path, normally
  `\\wsl.localhost\Ubuntu/mnt/snaport`. The mount disappears when the
  WSL VM stops; re-run the script to restore it. Unmount with
  `wsl -u root umount /mnt/snaport && wsl -u root losetup -D`.
  ext4-only images can also be browsed directly in 7-Zip (Open archive),
  but 7-Zip does not understand XFS.

- **NTFS/Windows volumes** - mount with [OSFMount](https://www.osforensics.com/tools/mount-disk-images.html)
  or ImDisk to get a drive letter, or convert to VHD and attach via
  Disk Management.

## Cost estimation

The EBS Direct APIs bill per request (about $0.003 per 1,000 requests each
for `ListSnapshotBlocks` and `GetSnapshotBlock`), and downloading to a
machine outside AWS adds internet data-transfer-out charges for the
allocated bytes. `--dry-run` prints an itemized estimate:

```
  cost est:    $4.08 (2 list + 81,920 get requests; egress 40.0 GB @ $0.09/GB = $3.60)
```

The estimate counts happy-path requests only — one `GetSnapshotBlock` per
allocated block plus the listing pages — so throttled retries and
token-refresh re-lists add marginally. Allocated bytes are an upper-bound
estimate (the listing exposes indexes, not lengths). Egress uses
`--egress-per-gb` (default $0.09/GB, roughly the us-east-1 internet-out
rate); set it to your region's rate, or `0` when running inside AWS or
through a VPC endpoint where egress is free. Verify current pricing at
<https://aws.amazon.com/ebs/pricing/>.

## Resume semantics

Interrupted runs (Ctrl-C, crash, reboot) resume safely:

- Progress is recorded in an append-only journal (`*.state.jsonl`) next to
  the image: one line per completed block (ordinal, length, SHA-256).
- The journal is only flushed **after** the image is fsynced, so anything
  the journal claims done is genuinely durable; anything not journaled is
  simply re-downloaded.
- Block tokens expire — resuming re-lists the snapshot for fresh tokens,
  and workers refresh tokens on demand mid-download.
- Snapshots are immutable, so an existing manifest is validated against
  the fresh listing; mismatches (e.g. a different snapshot re-using the
  same file name) refuse to proceed without `--force`.
- `--paranoid-resume` additionally re-reads a sample of completed blocks
  and re-downloads any that no longer match their recorded checksum.

Re-running a completed download is a no-op (no re-transfer) and can be
followed by `snaport verify` at any time.

## IAM policy

Minimal read-only policy for plain snapshots:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SnaportReadBlocks",
      "Effect": "Allow",
      "Action": ["ebs:ListSnapshotBlocks", "ebs:GetSnapshotBlock"],
      "Resource": "arn:aws:ec2:*::snapshot/*"
    }
  ]
}
```

Add for AMI downloads:

```json
    {
      "Sid": "SnaportResolveAmi",
      "Effect": "Allow",
      "Action": "ec2:DescribeImages",
      "Resource": "*"
    }
```

Optional but recommended: `ec2:DescribeSnapshots` lets snaport detect
pending/erroring snapshots up front (without it, snaport proceeds and
surfaces the raw API error instead):

```json
    {
      "Sid": "SnaportCheckState",
      "Effect": "Allow",
      "Action": "ec2:DescribeSnapshots",
      "Resource": "*"
    }
```

Note that the EBS Direct read APIs only serve **completed** snapshots;
downloading one that is still `pending` fails fast with a clear message,
or waits with `--wait 15m`.

For **encrypted** snapshots the caller also needs `kms:Decrypt` on the
snapshot's KMS key (the EBS Direct APIs return decrypted block data):

```json
    {
      "Sid": "SnaportKms",
      "Effect": "Allow",
      "Action": "kms:Decrypt",
      "Resource": "arn:aws:kms:<region>:<account>:key/<key-id>"
    }
```

## Throttling and quotas

The EBS Direct APIs are subject to account quotas (throttling on
`ListSnapshotBlocks` / `GetSnapshotBlock`). snaport layers three defenses:
the AWS SDK's adaptive retryer, per-block exponential backoff with jitter,
and AIMD concurrency control — parallelism is halved while throttled and
ramps back up when calm. If you consistently throttle, lower
`--concurrency`.

## Verification model

- **Per block**: the SHA-256 returned by `GetSnapshotBlock` (base64) must
  match the downloaded bytes; mismatches are refetched, then fail loudly.
- **Whole image**: re-read after download — every block checksum plus a
  SHA-256 over the full logical image (holes as zeros) recorded in the
  manifest.
- **Compressed artifact**: decoded end-to-end after compression; the
  decoded stream's hash must equal the image hash (this is what gates
  deletion of the raw image).
- `snaport verify` re-runs raw-image or archive verification on demand.

## Limitations / future work

- v1 downloads full snapshots; incremental re-sync via
  `ListChangedBlocks` is future work.
- Compression is a whole-image zstd stream (universally decodable); a
  seekable/chunked archive would allow random access without full
  decompression.
- The manifest records every block as JSON; multi-tens-of-millions of
  blocks (fully-allocated very large volumes) makes it large (~100 B/block).
- Streaming download→compression without materializing the raw image
  would eliminate the peak double-hold.
- Windows: the console progress display works in Windows Terminal and
  conhost; redirected output gets periodic plain lines instead.

## Development

```
go test ./...     # unit + integration tests (offline; no AWS account needed)
go vet ./...
snaport selftest  # the same offline end-to-end gate as the test suite
```

`internal/testutil` provides an in-memory `ebsx.Source` fake that
exercises pagination, token expiry, throttling injection and crash/resume
paths against the real download pipeline.
