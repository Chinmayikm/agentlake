# Flink Documentation

Apache Flink is a framework for stateful computations over data streams.

## Checkpointing

Checkpoints make state in Flink fault tolerant by allowing state and the
corresponding stream positions to be recovered.

### Checkpoint Storage

Checkpoints are persisted to a configured checkpoint storage location.

## Watermarks

Watermarks are used to reason about time in event-time processing.
