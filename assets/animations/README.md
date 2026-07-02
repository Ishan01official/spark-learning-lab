# Animated SVG Explainers

These files are self-contained HTML pages with inline SVG and CSS animations. They are intentionally
subtle: the motion highlights execution order, data movement, and recovery concepts without turning
the lesson into a flashy demo.

## Assets

| File | Concept | What moves |
| --- | --- | --- |
| [`lazy_evaluation_animation.html`](./lazy_evaluation_animation.html) | Lazy evaluation | Transformations appear first, then the action triggers execution |
| [`shuffle_animation.html`](./shuffle_animation.html) | Shuffle | Records move from input partitions through an exchange to output partitions |
| [`delta_lake_animation.html`](./delta_lake_animation.html) | Delta Lake | Commits update the log, then readers build a snapshot |
| [`streaming_animation.html`](./streaming_animation.html) | Structured Streaming | Micro-batches move from source to processing to sink while checkpoint pulses |
| [`medallion_animation.html`](./medallion_animation.html) | Medallion architecture | Records refine from Bronze to Silver to Gold |

## Notes

- No external JavaScript, CSS, fonts, or image dependencies.
- Animations use CSS keyframes only.
- Each file can be opened directly in a browser or embedded in a docs site.
