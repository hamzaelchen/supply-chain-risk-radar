"""Fonctions pures autour des fichiers GDELT 2.0 (aucune E/S ici).

GDELT publie un fichier d'événements toutes les 15 minutes, nommé par un
horodatage UTC : 20261008171500.export.CSV.zip. `lastupdate.txt` ne décrit que
le dernier créneau ; les créneaux précédents se reconstruisent par leur nom.
"""

import io
import re
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

BASE_URL = "http://data.gdeltproject.org/gdeltv2/"
LASTUPDATE_URL = BASE_URL + "lastupdate.txt"
EXPORT_SUFFIX = ".export.CSV.zip"
SLOT = timedelta(minutes=15)

_SLOT_RE = re.compile(r"/(\d{14})\.[^/]+$")


@dataclass(frozen=True)
class GdeltFile:
    url: str
    slot_ts: datetime
    # Connus seulement pour le créneau listé dans lastupdate.txt.
    size_bytes: int | None = None
    md5: str | None = None


def slot_from_url(url: str) -> datetime:
    match = _SLOT_RE.search(url)
    if not match:
        raise ValueError(f"horodatage introuvable dans l'URL : {url}")
    return datetime.strptime(match.group(1), "%Y%m%d%H%M%S").replace(tzinfo=UTC)


def parse_lastupdate(text: str) -> list[GdeltFile]:
    """Chaque ligne : `<taille> <md5> <url>` (export, mentions, gkg)."""
    files = []
    for line in text.splitlines():
        if not line.strip():
            continue
        size, md5, url = line.split()
        files.append(GdeltFile(url=url, slot_ts=slot_from_url(url), size_bytes=int(size), md5=md5))
    return files


def latest_export(text: str) -> GdeltFile:
    exports = [f for f in parse_lastupdate(text) if f.url.endswith(EXPORT_SUFFIX)]
    if len(exports) != 1:
        raise ValueError(f"lastupdate.txt : {len(exports)} fichier(s) export, 1 attendu")
    return exports[0]


def export_file_for_slot(slot_ts: datetime) -> GdeltFile:
    name = slot_ts.astimezone(UTC).strftime("%Y%m%d%H%M%S") + EXPORT_SUFFIX
    return GdeltFile(url=BASE_URL + name, slot_ts=slot_ts)


def slots_to_process(
    latest: datetime,
    first_known: datetime | None,
    done: set[datetime],
    max_backfill: int,
) -> tuple[list[datetime], int]:
    """Créneaux à traiter, du plus ancien au plus récent, et nombre laissés de côté.

    - Manifeste vide : seulement le dernier créneau (pas de rattrapage historique).
    - Sinon : tous les créneaux non terminés depuis le premier connu, dans la
      limite de `max_backfill` créneaux en arrière. Les plus anciens sont comptés
      dans le second élément pour être signalés, pas traités.
    """
    if first_known is None:
        return ([] if latest in done else [latest]), 0

    window_start = max(first_known, latest - SLOT * max_backfill)
    todo = []
    slot = window_start
    while slot <= latest:
        if slot not in done:
            todo.append(slot)
        slot += SLOT

    out_of_window = 0
    slot = first_known
    while slot < window_start:
        if slot not in done:
            out_of_window += 1
        slot += SLOT
    return todo, out_of_window


def iter_event_lines(zip_bytes: bytes) -> Iterator[bytes]:
    """Lignes TSV brutes du zip, octet pour octet, sans fin de ligne."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        names = archive.namelist()
        if len(names) != 1:
            raise ValueError(f"zip GDELT : {len(names)} fichiers, 1 attendu")
        with archive.open(names[0]) as member:
            for line in member:
                line = line.rstrip(b"\r\n")
                if line:
                    yield line


def event_key(line: bytes) -> bytes:
    """GLOBALEVENTID, première colonne : sert de clé Kafka."""
    return line.split(b"\t", 1)[0]


def raw_object_key(file: GdeltFile) -> str:
    day = file.slot_ts.astimezone(UTC).strftime("%Y-%m-%d")
    return f"raw/gdelt/events/date={day}/{file.url.rsplit('/', 1)[-1]}"
