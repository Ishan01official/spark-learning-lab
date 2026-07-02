# Visual Learning Assets

This folder indexes the real visual assets used by the Spark learning lab.

The primary outputs are **standalone SVG explainers** and **animated HTML/SVG explainers**. Prompt
files are kept as optional source material for future generated illustrations, but they are not the
main learning asset.

## Start here

- [Hand-drawn SVG gallery](./gallery.md) - embeds every SVG illustration from
  `assets/illustrations/handdrawn/`
- [Animation gallery](./animation_gallery.md) - links every standalone animated HTML/SVG explainer
- [Prompt archive](./hand_drawn_prompts/) - optional image-generation prompts for future raster art

## Visual style

The real SVG assets use a hand-drawn technical explainer style:

- white/light paper background
- rough black sketch lines
- slightly imperfect boxes and curved arrows
- small red and blue annotation accents
- readable labels
- one concept per image
- no external assets, libraries, or CDN dependencies

## Where assets live

```text
assets/illustrations/handdrawn/   # real SVG explainers
assets/animations/                # standalone animated HTML/SVG explainers
docs/visuals/gallery.md           # SVG gallery
docs/visuals/animation_gallery.md # animation index
```

## Recommended study loop

1. Open a Mermaid concept page under [`diagrams/`](../../diagrams/README.md).
2. Study the embedded hand-drawn SVG.
3. Open the related animation for the execution sequence.
4. Run the matching PySpark example.
5. Redraw the visual from memory and explain it out loud.
