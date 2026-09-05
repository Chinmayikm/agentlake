"""The evaluation harness -- agentlake's quality layer. See docs/adr/ADR-008.

Nothing here is imported by services/; the dependency runs one way, so the
thing being measured cannot change behaviour because it is being measured.
"""
