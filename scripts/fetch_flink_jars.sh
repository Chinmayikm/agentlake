#!/usr/bin/env bash
# Fetch the four connector/format jars the Flink containers need, into
# stream/flink/lib/ (gitignored -- ~126 MiB, regenerable). Run once:
#
#     make flink-jars
#
# docker-compose.yml bind-mounts these four files individually into
# /opt/flink/lib on the JobManager, TaskManager and SQL client. Individually,
# not as a directory: mounting a directory over /opt/flink/lib would shadow
# Flink's own jars and the container would not start.
#
# Checksums are pinned here rather than fetched alongside the jar -- a checksum
# downloaded from the same host in the same run attests to nothing. Values were
# read from Maven Central on 2026-08-28 and are the strongest digest *upstream
# publishes for that artifact*: Iceberg publishes .sha256, the Flink project
# still publishes only .md5/.sha1 (verified: .sha256 and .sha512 both 404), so
# the two Flink jars are pinned on sha1. That is upstream's floor, not a choice.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIB_DIR="$REPO_ROOT/stream/flink/lib"
BASE="https://repo1.maven.org/maven2"

# path-under-maven-central | digest-algorithm | digest
JARS=(
  "org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar|sha1|266330f8f26fec4957339b5ff31aa67b81c31462"
  "org/apache/flink/flink-sql-avro-confluent-registry/1.20.5/flink-sql-avro-confluent-registry-1.20.5.jar|sha1|abca6af761dbe802868ca9e77b4a40ac5a491c12"
  "org/apache/iceberg/iceberg-flink-runtime-1.20/1.10.1/iceberg-flink-runtime-1.20-1.10.1.jar|sha256|f06b3f2fbd004feeb10adc8957f27d43203a0dc526a9ae2e0a42219fbcdbcfe7"
  "org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar|sha256|86bf20892ea5b4c17688f19b075399885f6aa5303f6b2dc9f491e76ceef9633b"
  # Iceberg's Flink catalog factory takes a org.apache.hadoop.conf.Configuration
  # even for a REST catalog that never touches Hadoop, so Hadoop has to be on
  # the classpath or `CREATE CATALOG ... 'type'='iceberg'` dies with
  # ClassNotFoundException. These two shaded jars are the whole of it -- the
  # alternative Iceberg's docs suggest, flink-shaded-hadoop-2-uber, is a single
  # jar but pins Hadoop 2.8.3 (2018) and is deprecated on the Flink side.
  "org/apache/hadoop/hadoop-client-api/3.4.2/hadoop-client-api-3.4.2.jar|sha1|d49afafdccb52bddde866ea0f341e6b31edc97fe"
  "org/apache/hadoop/hadoop-client-runtime/3.4.2/hadoop-client-runtime-3.4.2.jar|sha1|3e9508f154ac9f085f3f0400c696175dce771d2d"
)

digest_of() {  # $1 = algorithm, $2 = file
  case "$1" in
    sha1)   sha1sum "$2" | cut -d' ' -f1 ;;
    sha256) sha256sum "$2" | cut -d' ' -f1 ;;
    *)      echo "unknown digest algorithm: $1" >&2; return 1 ;;
  esac
}

mkdir -p "$LIB_DIR"

for entry in "${JARS[@]}"; do
  IFS='|' read -r path algo want <<<"$entry"
  name="$(basename "$path")"
  dest="$LIB_DIR/$name"

  if [[ -f "$dest" ]] && [[ "$(digest_of "$algo" "$dest")" == "$want" ]]; then
    echo "ok (cached)  $name"
    continue
  fi

  echo "fetching     $name"
  # Download to a temp name and move only after the digest matches, so an
  # interrupted run never leaves a truncated jar that looks cached.
  curl -fsSL --retry 3 -o "$dest.part" "$BASE/$path"

  got="$(digest_of "$algo" "$dest.part")"
  if [[ "$got" != "$want" ]]; then
    rm -f "$dest.part"
    echo "FAILED       $name: $algo mismatch" >&2
    echo "  expected $want" >&2
    echo "  got      $got" >&2
    exit 1
  fi
  mv "$dest.part" "$dest"
  echo "ok           $name ($algo verified)"
done

echo
du -ch "$LIB_DIR"/*.jar | tail -1
