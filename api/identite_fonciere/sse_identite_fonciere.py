"""
Formatage SSE pour l’identité foncière (progression couche par couche).
"""
import json
from typing import Any, Dict, Iterator, Optional

from .identite_fonciere import iter_identite_fonciere_sse_events


def iter_identite_fonciere_sse_chunks(
    geometry: Dict[str, Any],
    commune: str,
    insee: Optional[str],
    srid: Optional[int],
) -> Iterator[str]:
    """Yield des lignes `data: {...}\\n\\n` pour `StreamingResponse`."""
    for ev in iter_identite_fonciere_sse_events(geometry, commune, insee, srid):
        yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"


def sse_error_chunk(message: str) -> str:
    payload: Dict[str, Any] = {
        "type": "error",
        "success": False,
        "error": message,
    }
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
