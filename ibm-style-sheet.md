# IBM-Inspired Document Style Guide

## What This Is (and What It Is Not)

This style guide describes the design system used in **standalone HTML documents** — project overviews, assignment specs, reference sheets, and lab materials that open in a browser directly rather than being embedded in Canvas.

> **Important distinction from `style-guide.txt`:** Canvas-embedded pages require all-inline styles and cannot touch `<body>` or `<h1>`. Standalone documents like `project.html` use a `<style>` block in `<head>` and a full custom design system. The two approaches are mutually exclusive — pick the right one for where the content lives.

The design is inspired by IBM Design Language: high-contrast ink on near-white, IBM Plex type family throughout, structured whitespace, and clean monochrome hierarchy. It reads like a well-typeset technical manual, not a slide deck.

---

## Guiding Principles

1. **Type does the work.** The font family carries authority. Keep color restrained — ink dark, accents purposeful.
2. **No emojis. Ever.** Icons can be SVG. Status can be conveyed with color, label text, and border treatment. Emojis cheapen technical documents.
3. **Structure is content.** Part numbers, task numbers, monospace labels — these tell students where they are. Use them consistently.
4. **Density is not clutter.** A dense page with clear hierarchy reads better than an airy page that buries the signal. Trust the reader.
5. **Neutral by default, signal by exception.** Callout boxes, colored task headers, and responsibility badges all mean something specific. Do not decorate for decoration's sake.

---

## Google Fonts Import

Always load the IBM Plex family. All three cuts are used in the system:

```html
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:ital,wght@0,400;0,500;0,600;1,400&family=IBM+Plex+Serif:wght@400;600&display=swap" rel="stylesheet">
```

| Typeface | Usage |
|---|---|
| IBM Plex Serif | Document title (`h1`), part headings (`h2.part`). Authority, weight. |
| IBM Plex Sans | Body copy, section headings (`h3`), table headers, labels. Readable at small sizes. |
| IBM Plex Mono | Metadata bars, subsection headings, code, tag-style labels (TASK 1, PROVIDED, etc.), numeric callout labels. |

Never substitute system fonts in this system. The Plex family is the identity.

---

## Design Tokens (CSS Custom Properties)

Declare these on `:root`. Every color used in the system references one of these — no ad-hoc hex values in component rules.

```css
:root {
  /* Ink */
  --ink:       #1a1a2e;   /* Main text, headings, rule lines */
  --ink-soft:  #4a4a6a;   /* Secondary text, table body copy */
  --ink-faint: #8888aa;   /* Metadata, captions, disabled labels */

  /* Surfaces */
  --rule:      #e0e0ee;   /* Borders, horizontal rules */
  --bg:        #fafafa;   /* Page background */
  --bg-warm:   #fffdf7;   /* Optional warm surface (rarely needed) */
  --code-bg:   #f0f0f8;   /* Code block and inline code background */

  /* Accent — Blue (Information, navigation, section headings) */
  --accent:    #1a4fa0;
  --accent-lt: #e8eef9;

  /* Gold (Emphasis, worked examples, latency notes, "you write" indicators) */
  --gold:      #b07d20;
  --gold-lt:   #fdf6e3;

  /* Green (Provided infrastructure, success states, notes) */
  --green:     #1a6b3a;
  --green-lt:  #eaf5ee;

  /* Red (Warnings, stop-and-read requirements) */
  --red:       #9b1c1c;
  --red-lt:    #fef2f2;

  /* Semantic aliases — use these in components, not the raw colors */
  --you:       var(--gold);       /* "You write" / student responsibility */
  --provided:  var(--green);      /* Provided by infrastructure / SAM */
}
```

### Color Meanings (Semantic Rules)

These are not decorative. Each color carries a meaning and must be used consistently:

| Color | Meaning | Use for |
|---|---|---|
| Blue (accent) | Information, structure | Section headings, links, info callouts |
| Gold | Student work / emphasis | "You Write" badges, worked examples, latency callouts, `callout.gold` |
| Green | Provided / safe | "Provided" badges, success callouts, positive notes, `callout.green` |
| Red | Warning / required read | Pre-conditions, stop-and-read warnings, `callout.red` |
| Ink | Neutral authority | Document title, part headings, task headers, table headers |

---

## Body and Layout

```css
body {
  font-family: 'IBM Plex Sans', sans-serif;
  font-size: 15px;
  line-height: 1.75;
  color: var(--ink);
  background: var(--bg);
  max-width: 860px;
  margin: 0 auto;
  padding: 60px 48px 120px;
}
```

- **860px max-width** is the sweet spot for dense technical content — wide enough for two-column tables, narrow enough to stay readable.
- **15px / 1.75** is the base. Do not shrink body copy below 14px. Do not use 16px+ for running text (it feels like a children's book).
- **120px bottom padding** gives breathing room after the footer.

---

## Typography Hierarchy

### Document Header

```css
.doc-meta {
  font-family: 'IBM Plex Mono', monospace;
  font-size: 11px;
  letter-spacing: .08em;
  text-transform: uppercase;
  color: var(--ink-faint);
  margin-bottom: 12px;
}

h1.doc-title {
  font-family: 'IBM Plex Serif', serif;
  font-size: 34px;
  font-weight: 600;
  line-height: 1.2;
  color: var(--ink);
  margin-bottom: 10px;
}

.doc-sub {
  font-size: 14px;
  color: var(--ink-soft);
}

.doc-header {
  border-bottom: 3px solid var(--ink);
  padding-bottom: 28px;
  margin-bottom: 52px;
}
```

The header is the only place a 3px border appears. It declares "this is a serious document." Everything else uses 1–2px rules.

### Part Headings (h2)

Part headings are the primary navigation anchors of the document. They carry a small-caps prefix label (e.g., "Part 1") so students can orient themselves quickly.

```css
h2.part {
  font-family: 'IBM Plex Serif', serif;
  font-size: 22px;
  font-weight: 600;
  color: var(--ink);
  margin: 56px 0 20px;
  padding-bottom: 8px;
  border-bottom: 2px solid var(--ink);
  display: flex;
  align-items: baseline;
  gap: 12px;
}

h2.part .part-num {
  font-family: 'IBM Plex Mono', monospace;
  font-size: 11px;
  letter-spacing: .1em;
  text-transform: uppercase;
  color: var(--ink-faint);
  font-weight: 400;
}
```

Usage:
```html
<h2 class="part"><span class="part-num">Part 3</span> Setup and Deployment</h2>
```

### Section Headings (h3)

Blue. IBM Plex Sans. This is the workhorse heading — use it within a Part to introduce major subsections.

```css
h3.section {
  font-family: 'IBM Plex Sans', sans-serif;
  font-size: 16px;
  font-weight: 600;
  color: var(--accent);
  margin: 32px 0 10px;
}
```

### Subsection Headings (h4)

Monospace. Used to label named sub-concepts within a section — scoring tables, individual steps, named parameters.

```css
h4.subsection {
  font-family: 'IBM Plex Mono', monospace;
  font-size: 13px;
  font-weight: 600;
  color: var(--ink);
  margin: 24px 0 8px;
  letter-spacing: .02em;
}
```

### Heading Hierarchy Summary

| Level | Class | Font | Size | Color | When to use |
|---|---|---|---|---|---|
| h1 | `.doc-title` | IBM Plex Serif | 34px | `--ink` | Document title only — once per page |
| h2 | `.part` | IBM Plex Serif | 22px | `--ink` | Major document sections (Part 1, Part 2…) |
| h3 | `.section` | IBM Plex Sans | 16px | `--accent` | Subsections within a Part |
| h4 | `.subsection` | IBM Plex Mono | 13px | `--ink` | Named sub-concepts, named items |

Never skip a level. Never use a level just to get the visual weight — rethink the structure instead.

---

## Callout Boxes

Callouts are the primary semantic highlighting tool. They come in four colors, each with a defined meaning.

```css
.callout {
  border-left: 4px solid var(--accent);
  background: var(--accent-lt);
  padding: 16px 20px;
  margin: 24px 0;
  border-radius: 0 6px 6px 0;   /* flat left, rounded right */
}
.callout.gold  { border-color: var(--gold);  background: var(--gold-lt);  }
.callout.green { border-color: var(--green); background: var(--green-lt); }
.callout.red   { border-color: var(--red);   background: var(--red-lt);   }

.callout-label {
  font-family: 'IBM Plex Mono', monospace;
  font-size: 10px;
  letter-spacing: .1em;
  text-transform: uppercase;
  font-weight: 600;
  margin-bottom: 6px;
  color: var(--ink-soft);
}
.callout p { font-size: 14px; }
```

Usage:
```html
<div class="callout gold">
  <div class="callout-label">Scoring Formula</div>
  <p>score = bid_amount × relevance_multiplier × time_bonus × device_bonus</p>
</div>

<div class="callout red">
  <div class="callout-label">Before You Begin</div>
  <p>Run the Burst profile before collecting data for analysis.</p>
</div>
```

**When to use each:**
- **Default (blue):** General notes, important definitions, "how this works"
- **Gold:** Formulas, worked examples, latency/performance notes, anything the student actively uses
- **Green:** Notes about provided infrastructure, confirmation messages, tips
- **Red:** Hard prerequisites, stop-and-read warnings, data loss risks

---

## Responsibility Bands

Used to show the student their role relative to what is already built. Three panels side by side, color-coded by actor.

```css
.responsibility {
  display: flex;
  margin: 28px 0;
  border: 1.5px solid var(--rule);
  border-radius: 8px;
  overflow: hidden;
}
.resp-item { flex: 1; padding: 16px 18px; font-size: 13px; line-height: 1.5; }
.resp-item .resp-label {
  font-family: 'IBM Plex Mono', monospace;
  font-size: 10px;
  letter-spacing: .1em;
  text-transform: uppercase;
  font-weight: 600;
  margin-bottom: 6px;
}
.resp-item.provided { background: var(--green-lt); }
.resp-item.provided .resp-label { color: var(--provided); }
.resp-item.yours {
  background: var(--gold-lt);
  border-left: 3px solid var(--gold);
  border-right: 3px solid var(--gold);
}
.resp-item.yours .resp-label { color: var(--you); }
```

Usage:
```html
<div class="responsibility">
  <div class="resp-item provided">
    <div class="resp-label">Provided by SAM Template</div>
    SQS input queue, results queue, DynamoDB table, IAM role
  </div>
  <div class="resp-item yours">
    <div class="resp-label">You Write</div>
    Four functions in <code>lambda_handler.py</code>
  </div>
  <div class="resp-item provided">
    <div class="resp-label">You Write</div>
    Analyst report in <code>analysis.ipynb</code>
  </div>
</div>
```

The center panel always uses the gold/`yours` treatment. Flanking panels use green/`provided` even if the student is also responsible for them — it signals that those pieces are structural givens, not implementation tasks.

---

## Task Blocks

Task blocks are numbered implementation units — one per function or deliverable. The dark header rail gives them visual weight that matches their importance.

```css
.task { border: 1.5px solid var(--rule); border-radius: 8px; margin: 20px 0; overflow: hidden; }
.task-header {
  background: var(--ink);
  color: #fff;
  padding: 10px 18px;
  display: flex;
  align-items: center;
  gap: 14px;
}
.task-num {
  font-family: 'IBM Plex Mono', monospace;
  font-size: 10px;
  letter-spacing: .1em;
  text-transform: uppercase;
  opacity: .6;
}
.task-name {
  font-family: 'IBM Plex Mono', monospace;
  font-size: 13px;
  font-weight: 600;
}
.task-body { padding: 16px 18px; font-size: 14px; }
```

Usage:
```html
<div class="task">
  <div class="task-header">
    <span class="task-num">Task 1</span>
    <span class="task-name">compute_score(bid, opportunity)</span>
  </div>
  <div class="task-body">
    <p>Implement the scoring formula…</p>
  </div>
</div>
```

The function signature in `.task-name` renders in monospace, which makes it immediately clear this is a code artifact, not a prose title. Use this for any named deliverable: functions, CLI commands, notebook cells.

---

## Numbered Steps

For procedural content — deploy sequences, setup steps — use the step component. Numbered circle + body column.

```css
.step { display: flex; gap: 20px; margin: 20px 0; align-items: flex-start; }
.step-num {
  flex-shrink: 0;
  width: 32px; height: 32px;
  background: var(--accent);
  color: #fff;
  border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-family: 'IBM Plex Mono', monospace;
  font-size: 13px; font-weight: 600;
  margin-top: 2px;
}
.step-body { flex: 1; }
.step-body h4 { font-size: 15px; font-weight: 600; margin-bottom: 6px; color: var(--ink); }
.step-body p, .step-body ul { font-size: 14px; }
```

Usage:
```html
<div class="step">
  <div class="step-num">2</div>
  <div class="step-body">
    <h4>Deploy with guided setup</h4>
    <p>The first deploy uses <code>--guided</code> to save settings to <code>samconfig.toml</code>.</p>
    <pre>sam deploy --guided --stack-name adflow-YOURID</pre>
  </div>
</div>
```

Steps use blue circles (accent). Never gold or green — those are reserved for semantic meaning, not sequential numbering.

---

## Tables

Tables are used for reference material: message schemas, scoring tables, resource naming conventions, grading rubrics.

```css
.table-wrap { overflow-x: auto; margin: 18px 0; }
table { width: 100%; border-collapse: collapse; font-size: 13.5px; }

thead th {
  background: var(--ink);
  color: #fff;
  font-weight: 600;
  font-family: 'IBM Plex Sans', sans-serif;
  text-align: left;
  padding: 10px 14px;
  letter-spacing: .01em;
}
tbody tr:nth-child(even) { background: #f4f4f8; }
tbody td { padding: 9px 14px; border-bottom: 1px solid var(--rule); vertical-align: top; }
tbody td:first-child {
  font-family: 'IBM Plex Mono', monospace;
  font-size: 12px;
  color: var(--accent);
}
```

The first column of a table is always rendered in monospace blue. This works well for field names, resource names, command names, and grading criteria labels — items the reader will scan for.

Always wrap tables in `.table-wrap` for horizontal scroll on mobile.

---

## Code Blocks and Inline Code

```css
pre {
  background: var(--code-bg);
  border: 1px solid var(--rule);
  border-radius: 6px;
  padding: 18px 22px;
  margin: 16px 0;
  overflow-x: auto;
  font-family: 'IBM Plex Mono', monospace;
  font-size: 12.5px;
  line-height: 1.65;
  color: var(--ink);
}

code {
  font-family: 'IBM Plex Mono', monospace;
  font-size: 12.5px;
  background: var(--code-bg);
  padding: 1px 5px;
  border-radius: 3px;
}
```

Notes:
- Code blocks in this system use a **subtle violet-tinted background** (`#f0f0f8`) rather than a neutral gray. It subconsciously signals "technical content" without calling loud attention to itself.
- No syntax highlighting — the system is monochromatic and syntax highlighting would require a JS library or inline spans. Meaningful comments inside the code block are the preferred way to annotate.
- Inline `<code>` uses the same background and font at 12.5px with 1px/5px padding. Use it for: function names, field names, environment variable names, CLI flags, file paths.

---

## SVG Diagrams

Diagrams are authored inline SVG — no external dependencies, no server calls, works offline. They sit inside `.diagram` wrappers.

```css
.diagram { margin: 28px 0; text-align: center; }
.diagram svg { max-width: 100%; height: auto; }
.diagram-caption {
  font-size: 12px;
  color: var(--ink-faint);
  font-style: italic;
  margin-top: 8px;
  font-family: 'IBM Plex Sans', sans-serif;
}
```

### SVG Color Conventions

Use the same semantic colors as the rest of the system. Map them directly in your SVG attributes:

| Role | Fill | Stroke |
|---|---|---|
| Student code / "You Write" | `#fdf6e3` | `#b07d20` |
| Infrastructure / "Provided" | `#eaf5ee` | `#1a6b3a` |
| Information / navigation | `#e8eef9` | `#1a4fa0` |
| Warning block | `#fef2f2` | `#9b1c1c` |
| Arrows and connectors | none | `#8888aa` |
| Dashed outlines (regions) | tinted fill | dashed stroke at 1.2px |

### Arrowhead Pattern

Define a reusable arrowhead marker in each SVG's `<defs>`:

```svg
<defs>
  <marker id="arr1" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
    <path d="M0,0 L0,6 L8,3 z" fill="#8888aa"/>
  </marker>
</defs>
```

Then attach to lines: `marker-end="url(#arr1)"`. Use a unique `id` per SVG if you have multiple diagrams on the page (e.g., `arr1`, `arr2`, `arr3`).

### SVG Typography

All SVG text uses `font-family="'IBM Plex Sans', sans-serif"` declared on the `<svg>` element. Label text is 9–11px. Title text (centered at top of diagram) is 12px bold. Never use full-sentence prose inside an SVG box — keep box labels to 2–3 words maximum.

---

## Document Footer

```css
.doc-footer {
  margin-top: 72px;
  padding-top: 20px;
  border-top: 1px solid var(--rule);
  font-size: 13px;
  color: var(--ink-soft);
}
```

```html
<footer class="doc-footer">
  Assigmnet name &nbsp;·&nbsp; Class Name  &nbsp;·&nbsp; New College of Florida &nbsp;·&nbsp;  2026
</footer>
```

The footer uses a centered dot (`&nbsp;·&nbsp;`) as a separator — compact, well-spaced, readable. It mirrors the `doc-meta` style (monospace metadata) but uses IBM Plex Sans since it reads at a larger scale.

---

## Voice and Writing Conventions

Drawn from `style-guide.txt` and extended for technical project documents:

### Do

- **Write in complete sentences.** Even instructional bullet points should read as instructions, not fragments.
- **Use active voice.** "Your Lambda receives a batch" not "A batch is received by the Lambda."
- **Name things precisely.** If the field is `batchItemFailures`, write `batchItemFailures` — not "the failure key" or "that field."
- **State the consequence, not just the requirement.** "If you return an empty list, SQS considers every message successfully processed and deletes them from the queue" — not "return an empty list if all messages succeeded."
- **Use em dashes for asides** (`—`), not parentheses. They read more authoritatively.
- **Number sections.** Part 1, Part 2 / 1.1, 1.2 — so students can say "I'm stuck on 3.3" in office hours.
- **Label callout boxes.** Every callout box has a `.callout-label` in UPPERCASE MONOSPACE. It primes the reader for what type of information follows.

### Do Not

- **No emojis.** Not even for headers, bullets, or status indicators. See `style-guide.txt`: *"NEVER USE EMOJIS... NO REALLY NOT EVEN FOR HEADINGS."* This applies even more strongly in a formal technical document.
- **No "actually."** Avoid it entirely. It implies the reader was wrong, which is condescending.
- **No "just" or "simply."** They minimize effort students may genuinely find difficult.
- **No rhetorical questions as section openers.** ("But what is a queue?") Technical documents state; they do not ask.
- **No passive voice to avoid naming the actor.** If something happens, say what makes it happen.
- **No placeholders.** Every piece of example code must be syntactically valid and complete. Use `YOURID` as a consistent placeholder token when a student ID is required, never `<your-id-here>`.

---

## Canvas vs. Standalone — Decision Guide

| Situation | System to use |
|---|---|
| Content embedded inside Canvas page or assignment | `style-guide.txt` (inline styles, no `<style>` block) |
| Standalone HTML opened in browser, linked from Canvas | This guide (IBM system, `<style>` block, full design tokens) |
| Standalone HTML that will later be iframed in Canvas | This guide, but test at the iframe width (usually 820px) |
| Short reference card or cheat sheet | Either — prefer this guide for technical content |

The tell: if Canvas might strip your `<style>` tags, you are in Canvas territory. If the file is served as a raw HTML page, use this system.

---

## Quick-Start Template

Minimum viable document using this system:

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Your Document Title</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:ital,wght@0,400;0,500;0,600;1,400&family=IBM+Plex+Serif:wght@400;600&display=swap" rel="stylesheet">
<style>
  /* ── Paste the full :root block and all component rules here ── */
</style>
</head>
<body>

<header class="doc-header">
  <div class="doc-meta">Course Name &nbsp;·&nbsp; Institution &nbsp;·&nbsp; Term</div>
  <h1 class="doc-title">Document Title</h1>
  <div class="doc-sub">One-sentence description of what this document covers.</div>
</header>

<h2 class="part"><span class="part-num">Part 1</span> First Section</h2>
<p>Body copy…</p>

<footer class="doc-footer">
  Assignment Name &nbsp;·&nbsp; Course &nbsp;·&nbsp; Institution &nbsp;·&nbsp; Term
</footer>

</body>
</html>
```

The complete CSS for all components is in `student-starter/project.html` inside the `<style>` block — copy the entire block into your new document and remove rules for components you do not use.

---

## Anti-Patterns

These patterns look reasonable but break the system:

| Anti-pattern | Why it's wrong | Correct approach |
|---|---|---|
| Nested callouts | Destroys visual hierarchy | Use a single callout; put secondary content in a list inside it |
| More than two font sizes in running prose | Signals indecision, not structure | 15px body, 14px secondary — that's it |
| Orange or purple as a fifth semantic color | Dilutes the meaning of gold/green/red/blue | Add a new `.callout` variant only if you have a new semantic category |
| Task headers in blue (accent) instead of ink | Makes tasks read as section labels | Task headers are always `var(--ink)` — dark, heavy, unambiguous |
| Using `<h3>` without the `.section` class | Default browser h3 does not match the system | Always pair heading level with the correct class |
| Putting SVG diagrams in `<img>` tags | Loses accessibility and color consistency | Inline SVG always |
