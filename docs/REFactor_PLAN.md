# Refactor plan summary

## Removed / neutralized
- Legacy Ormanet and B2B e-commerce data models, endpoints, and wording were replaced by Seed AI primitives.
- UI labels and assets referencing offers, carts, or promo flows were removed.

## Kept
- aiohttp-based server scaffold and static file serving.
- Three.js-based 3D universe rendering and camera controls.
- SQLite persistence layer (now simplified for nodes/messages/logs).

## Added
- Node data model with hash IDs, 3D positions, metadata, and timestamps.
- Node messages and per-node logs stored in SQLite and exposed via REST APIs.
- Mock async LLM adapter ready to swap with a real Mistral 7B integration.
- Dark “Seed AI universe” GUI with chat side panel, node creation, reset, export, and child node shortcuts.
- README documenting setup, running, and the new API surface.
