"""
Post-processing content filter for search results.

Kleinanzeigen bietet keine serverseitige Volltextfilterung an. Dieses Modul
filtert bereits abgerufene Ergebnisse anhand von Include-/Exclude-Begriffen
oder Regex-Mustern in Titel, Vorschaubeschreibung oder (bei Detail-Fetches)
der vollständigen Beschreibung.
"""

import re
from typing import Any, Dict, List, Optional
from fastapi import HTTPException

VALID_FIELDS = {"title", "description", "description_full"}


class ContentFilter:
    def __init__(
        self,
        include_terms: Optional[str] = None,
        exclude_terms: Optional[str] = None,
        use_regex: bool = False,
        match_all_include: bool = False,
        case_sensitive: bool = False,
        filter_fields: str = "title,description",
    ):
        self.use_regex = use_regex
        self.match_all_include = match_all_include
        self.case_sensitive = case_sensitive

        fields = [f.strip() for f in filter_fields.split(",") if f.strip()]
        unknown = set(fields) - VALID_FIELDS
        if unknown:
            raise HTTPException(
                status_code=400,
                detail=f"Unbekannte filter_fields: {sorted(unknown)}. "
                       f"Erlaubt: {sorted(VALID_FIELDS)}",
            )
        self.fields = fields or ["title", "description"]

        flags = 0 if case_sensitive else re.IGNORECASE
        try:
            self.include_patterns = self._compile(include_terms, flags)
            self.exclude_patterns = self._compile(exclude_terms, flags)
        except re.error as e:
            raise HTTPException(status_code=400, detail=f"Ungültiges Regex-Muster: {e}")

    def _compile(self, terms: Optional[str], flags: int) -> List[re.Pattern]:
        if not terms:
            return []
        parts = [t.strip() for t in terms.split(",") if t.strip()]
        if self.use_regex:
            return [re.compile(p, flags) for p in parts]
        return [re.compile(re.escape(p), flags) for p in parts]

    @property
    def active(self) -> bool:
        return bool(self.include_patterns or self.exclude_patterns)

    @property
    def needs_full_description(self) -> bool:
        return "description_full" in self.fields

    def _text_for(self, listing: Dict[str, Any]) -> str:
        parts = []
        if "title" in self.fields:
            parts.append(listing.get("title") or "")
        if "description" in self.fields:
            parts.append(listing.get("description") or "")
        if "description_full" in self.fields:
            details = listing.get("details") or {}
            parts.append(details.get("description") or "")
        return "\n".join(parts)

    def matches(self, listing: Dict[str, Any]) -> bool:
        text = self._text_for(listing)

        if self.exclude_patterns and any(p.search(text) for p in self.exclude_patterns):
            return False

        if self.include_patterns:
            hits = [bool(p.search(text)) for p in self.include_patterns]
            return all(hits) if self.match_all_include else any(hits)

        return True

    def apply(self, listings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not self.active:
            return listings
        return [item for item in listings if self.matches(item)]

    def summary(self) -> Dict[str, Any]:
        return {
            "include_terms": [p.pattern for p in self.include_patterns],
            "exclude_terms": [p.pattern for p in self.exclude_patterns],
            "use_regex": self.use_regex,
            "match_all_include": self.match_all_include,
            "case_sensitive": self.case_sensitive,
            "filter_fields": self.fields,
        }


def content_filter_from_params(
    include_terms: Optional[str] = None,
    exclude_terms: Optional[str] = None,
    use_regex: bool = False,
    match_all_include: bool = False,
    case_sensitive: bool = False,
    filter_fields: str = "title,description",
) -> ContentFilter:
    return ContentFilter(
        include_terms=include_terms,
        exclude_terms=exclude_terms,
        use_regex=use_regex,
        match_all_include=match_all_include,
        case_sensitive=case_sensitive,
        filter_fields=filter_fields,
    )
