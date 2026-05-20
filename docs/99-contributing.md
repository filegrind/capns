---
title: "Contributing"
layout: doc
permalink: /99-contributing/
---
# Contributing

Anyone can propose a new capability or media def for the CAPDAG registry. The registry is community-curated, and as long as a definition is well-formed, fills a real gap, and is documented clearly enough that others can use it, it's a candidate for inclusion. Submissions happen through the GitHub issue tracker on the [`capfab`](https://github.com/machinefabric/capfab) repository, which is the canonical reference for the published JSON definitions.

## How submissions work

There is no automated merge. Every proposal is read by a maintainer; we work with the submitter on any rough edges; and once the shape is right, the new definition appears in [`capfab/standard/`](https://github.com/machinefabric/capfab/tree/main/standard) and on capdag.com.

You submit JSON, not source-format. Browse [`capfab/standard/`](https://github.com/machinefabric/capfab/tree/main/standard) and [`capfab/standard/media/`](https://github.com/machinefabric/capfab/tree/main/standard/media) for live examples of the JSON shape we accept. The schemas live at [`cap.schema.json`](https://github.com/machinefabric/capfab/blob/main/cap.schema.json) and [`media.schema.json`](https://github.com/machinefabric/capfab/blob/main/media.schema.json).

## What to submit

Open an issue on capfab using one of the templates below.

| To do this | Template |
| --- | --- |
| Add a new capability | [Add Capability](https://github.com/machinefabric/capfab/issues/new?template=add-capability.yml) |
| Add a new media def | [Add Media Def](https://github.com/machinefabric/capfab/issues/new?template=add-media-def.yml) |
| Remove a definition | [Remove Definition](https://github.com/machinefabric/capfab/issues/new?template=remove-definition.yml) |
| Edit an existing definition (typo, docs, metadata) | [Edit Existing Definition](https://github.com/machinefabric/capfab/issues/new?template=edit-definition.yml) |
| Report a bug, ask a question, propose a feature | [Bug / Feature / Question](https://github.com/machinefabric/capfab/issues/new?template=bug-or-feature.yml) |

The full template picker is at [github.com/machinefabric/capfab/issues/new/choose](https://github.com/machinefabric/capfab/issues/new/choose).

## What makes a good capability submission

- **A real, distinct operation.** Not a re-skinning of an existing capability with different non-directional tags. Other tags ride along on the URN, but only `in` and `out` are functionally significant — see [Predicates](/docs/04-predicates) and [Cap URN Structure](/docs/06-cap-urn-structure) for the matching semantics.
- **Documentation that says when to use it and when not to.** End users discover capabilities by reading the `documentation` field on capdag.com. A bare line like "convert X to Y" doesn't give them enough to decide. Tell them what upstream produces this kind of thing, what downstream consumes the output, and what neighbouring capabilities they might be confusing it with.
- **Argument shape that follows the patterns we already use.** Look at a handful of existing caps in [`standard/`](https://github.com/machinefabric/capfab/tree/main/standard) before writing yours.
- **A URN that survives review.** URNs are tagged URNs; once published, the URN is the permanent key. Picking the right tags up front matters.

## What makes a good media def submission

- **A type with a clear shape and a clear role.** Specs that overlap heavily with an existing one will be sent back to merge. If you think you need a new spec, explain in the rationale why an existing spec doesn't fit — what flow gets blocked, which existing type is too general or too specific.
- **A `media_type`, a `title`, and a `documentation` block.** The `documentation` should explain when to use this type vs. neighbouring types, not just describe what it represents in isolation.
- **Stable extensions if relevant.** For media types tied to file formats, list the canonical file extensions in `extensions` so file-based ingestion can map by extension.

## Removals and edits

Removals are weighed against impact. Definitions in the registry are referenced by deployed cartridges, by published capdag.com pages, and by users' saved machine notations. We will work with you on a deprecation path before removing anything that is in active use.

Edits change a definition's metadata (title, docs, content type, extensions) but never change its URN. The URN is the permanent key; renaming a URN is not an edit, it is a removal plus a fresh add.

## What's out of scope

- **Implementations of capabilities.** The capfab registry is a directory of capability and media definitions, not a place to publish cartridges or implementations. Cartridges are distributed separately. A capability can exist in the registry before any implementation exists.

## Conduct

Be kind. We're a small project trying to keep a shared vocabulary clean and useful. If a maintainer asks you to revise a submission, it's because we want to merge it, not because we don't.
