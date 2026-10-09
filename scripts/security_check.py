#!/usr/bin/env python3
"""
Comprehensive Secret and Credential Scanner for Glacier Ecosystem.
Scans files before git commit, push, or publish to ensure NO credentials or secrets are leaked.
"""

import os
import re
import sys
from pathlib import Path

# High-confidence secret regex patterns
SECRET_PATTERNS = [
    ("Google AI / Gemini / Maps API Key", re.compile(r'\bAIza[0-9A-Za-z-_]{35}\b')),
    ("OpenAI API Key", re.compile(r'\bsk-[a-zA-Z0-9]{20,}\b')),
    ("Anthropic API Key", re.compile(r'\bsk-ant-[a-zA-Z0-9-_]{20,}\b')),
    ("GitHub Personal Access Token", re.compile(r'\bgh[pousr]_[A-Za-z0-9_]{36,255}\b')),
    ("AWS Access Key ID", re.compile(r'\b(AKIA|ABIA|ACCA|ASIA)[0-9A-Z]{16}\b')),
    ("AWS Secret Access Key", re.compile(r'(?i)aws_secret_access_key\s*[:=]\s*["\']?([A-Za-z0-9/+=]{40})["\']?')),
    ("Private Key Block", re.compile(r'-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----')),
    ("HuggingFace Token", re.compile(r'\bhf_[a-zA-Z0-9]{34,}\b')),
    ("Slack Token", re.compile(r'\bxox[baprs]-[0-9a-zA-Z]{10,48}\b')),
    ("Generic Secret Assignment", re.compile(r'''(?i)(?:api[_-]?key|secret[_-]?key|access[_-]?token|auth[_-]?token|private[_-]?key|password|client[_-]?secret)\s*[:=]\s*["']([^"'\s]{12,})["']''')),
]

# Patterns that are known false positives / placeholders / test fixtures
ALLOWLIST_PATTERNS = [
    re.compile(r'(?i)your[-_]?api[-_]?key'),
    re.compile(r'(?i)placeholder'),
    re.compile(r'(?i)dummy'),
    re.compile(r'(?i)fake'),
    re.compile(r'(?i)sample'),
    re.compile(r'(?i)example'),
    re.compile(r'(?i)<[^>]+>'),
    re.compile(r'(?i)process\.env'),
    re.compile(r'(?i)environment\.getenvironmentvariable'),
    re.compile(r'(?i)config\["'),
    re.compile(r'(?i)\bAIzaSyDummy\b'),
    re.compile(r'(?i)Bearer\s+\$TOKEN'),
]

IGNORE_DIRS = {
    '.git', 'bin', 'obj', 'node_modules', '.vs', '.idea', '.vscode',
    'packages', 'BenchmarkDotNet.Artifacts', '.gemini', 'tmp',
    'nuget-local', 'dist', 'scratch_challenger_2', 'scratch_challenger_3'
}

IGNORE_EXTENSIONS = {
    '.exe', '.dll', '.pdb', '.so', '.dylib', '.png', '.jpg', '.jpeg',
    '.gif', '.ico', '.pdf', '.zip', '.tar', '.gz', '.nupkg', '.gguf',
    '.bin', '.parquet', '.feather', '.mp4', '.mp3', '.wav', '.mkv',
    '.avi', '.flac', '.pt', '.pth', '.safetensors', '.onnx', '.csr'
}

def is_allowlisted(match_str: str, line: str) -> bool:
    for pat in ALLOWLIST_PATTERNS:
        if pat.search(match_str) or pat.search(line):
            return True
    return False

def scan_file(file_path: Path):
    findings = []
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line_no, line in enumerate(f, start=1):
                # Skip comments that explicitly discuss security patterns or tests
                if "SECRET_PATTERNS" in line or "AIza[0-9A-Za-z" in line:
                    continue
                for name, pattern in SECRET_PATTERNS:
                    matches = pattern.finditer(line)
                    for m in matches:
                        matched_val = m.group(0)
                        if not is_allowlisted(matched_val, line):
                            findings.append((line_no, name, matched_val[:8] + "..." + matched_val[-4:] if len(matched_val) > 12 else "[REDACTED]"))
    except Exception as e:
        pass
    return findings

def scan_directory(root_dir: str):
    total_scanned = 0
    total_findings = []
    root = Path(root_dir)

    for dirpath, dirnames, filenames in os.walk(root):
        # Prune ignored directories
        dirnames[:] = [d for d in dirnames if d not in IGNORE_DIRS and not d.startswith('.')]

        for fname in filenames:
            fpath = Path(dirpath) / fname
            if fpath.suffix.lower() in IGNORE_EXTENSIONS:
                continue
            if fpath.name.endswith('.min.js') or fpath.name.endswith('.min.css'):
                continue
            # Skip this script itself
            if fpath.name == 'security_check.py':
                continue

            findings = scan_file(fpath)
            total_scanned += 1
            if findings:
                for line_no, secret_type, redacted in findings:
                    total_findings.append((str(fpath.relative_to(root)), line_no, secret_type, redacted))

    return total_scanned, total_findings

def main():
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    target = sys.argv[1] if len(sys.argv) > 1 else "."
    print(f"[SECURITY] Running Glacier Secrets Scanner on: {os.path.abspath(target)}", flush=True)
    scanned, findings = scan_directory(target)
    print(f"  Scanned {scanned} files.", flush=True)

    if findings:
        print("\n" + "="*70, flush=True)
        print("[CRITICAL ALERT] POTENTIAL SECRETS DETECTED!", flush=True)
        print("="*70, flush=True)
        for fpath, line_no, sec_type, redacted in findings:
            print(f"  FAILED: {fpath}:{line_no} -> {sec_type} ({redacted})", flush=True)
        print("="*70, flush=True)
        print("Commit/Publish BLOCKED. Please remove or sanitize secrets before proceeding.", flush=True)
        sys.exit(1)
    else:
        print("  [PASSED] Zero secrets or credentials detected. Safe to commit/publish.", flush=True)
        sys.exit(0)

if __name__ == '__main__':
    main()
