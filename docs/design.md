# Streaming / Production Architecture for Utopia Video Cameras

- Bronze: raw events (with duplicates).
- Silver: streaming job does watermark + dropDuplicates on detection_oid, then broadcast-join B.
- Gold: aggregated top-N per location for dashboards.
- Dashboard reads from Gold, so it is always deduplicated and enriched.
