from dataclasses import dataclass
from typing import Dict, List

@dataclass
class ParsedBlock:
    entity: str
    header: List[str]
    rows: List[List[str]]

class InterleavedParseError(ValueError):
    pass

def parse_interleaved_text(content: str) -> Dict[str, ParsedBlock]:
    """Parse HDR/FTR blocks without depending on entity order."""
    blocks: Dict[str, ParsedBlock] = {}
    current_entity=None; current_header=None; current_rows=[]
    for line_number, raw_line in enumerate(content.splitlines(),start=1):
        line=raw_line.strip()
        if not line: continue
        parts=[part.strip() for part in line.split("|")]
        record_type=parts[0].upper()
        if record_type=="HDR":
            if len(parts)<2: raise InterleavedParseError(f"Line {line_number}: HDR missing entity name")
            if current_entity is not None: raise InterleavedParseError(f"Line {line_number}: new HDR found before FTR for {current_entity}")
            current_entity=parts[1].upper(); current_header=None; current_rows=[]; continue
        if record_type=="FTR":
            if current_entity is None: raise InterleavedParseError(f"Line {line_number}: FTR found without active HDR")
            footer_entity=parts[1].upper() if len(parts)>1 else current_entity
            if footer_entity!=current_entity: raise InterleavedParseError(f"Line {line_number}: FTR {footer_entity} does not match {current_entity}")
            if current_header is None: raise InterleavedParseError(f"Entity {current_entity} has no header row")
            if current_entity in blocks:
                existing=blocks[current_entity]
                if existing.header!=current_header: raise InterleavedParseError(f"Repeated entity {current_entity} has a different header")
                existing.rows.extend(current_rows)
            else: blocks[current_entity]=ParsedBlock(current_entity,current_header,current_rows)
            current_entity=None; current_header=None; current_rows=[]; continue
        if current_entity is None: raise InterleavedParseError(f"Line {line_number}: data found outside an HDR/FTR block")
        if current_header is None: current_header=parts
        else:
            if len(parts)!=len(current_header): raise InterleavedParseError(f"Line {line_number}: {current_entity} expected {len(current_header)} fields, got {len(parts)}")
            current_rows.append(parts)
    if current_entity is not None: raise InterleavedParseError(f"File ended before FTR for {current_entity}")
    return blocks
