import argparse
import importlib
import json
import logging
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv


LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description='Send latest management reports via Microsoft Graph from CLI')
    parser.add_argument(
        '--dispatch-type',
        choices=['management', 'core', 'usa'],
        default='management',
        help='Dispatch profile: management (combined), core (core markets), or usa (USA spa)'
    )
    parser.add_argument('--to', required=False, help='Recipient email or comma/semicolon separated list')
    parser.add_argument('--recipient-group', required=False, help='Recipient group key from recipients JSON (e.g. management/core/usa/test)')
    parser.add_argument('--recipients-json', default='config/dispatch_recipients.json', help='Path to recipients JSON file')
    parser.add_argument('--sender', default=None, help='Optional Graph sender mailbox (overrides REPORT_DISPATCH_GRAPH_SENDER)')
    parser.add_argument('--subject', default=None, help='Optional subject override')
    parser.add_argument('--body', default='Please find the latest QMS sales data attached.', help='Optional intro/body text')
    parser.add_argument('--outputs-dir', default=None, help='Optional outputs directory override')
    parser.add_argument('--refresh', action='store_true', help='Refresh reports before dispatch (runs configured refresh command)')
    parser.add_argument('--dry-run', action='store_true', help='Build and log payload details without sending')
    return parser.parse_args(argv)


def _wire_dispatch_imports(project_root: Path):
    azure_functions_root = project_root / 'azure_functions'
    if str(azure_functions_root) not in sys.path:
        sys.path.insert(0, str(azure_functions_root))

    config = importlib.import_module('dispatch_reports.config')
    graph_client = importlib.import_module('dispatch_reports.graph_client')
    html_builder = importlib.import_module('dispatch_reports.html_builder')
    report_collector = importlib.import_module('dispatch_reports.report_collector')

    return {
        'parse_recipients': config.parse_recipients,
        'report_date_str': config.report_date_str,
        'report_mtd_banner': config.report_mtd_banner,
        'send_via_graph': graph_client.send_via_graph,
        'build_html_body': html_builder.build_html_body,
        'collect_csv_attachments': report_collector.collect_csv_attachments,
        'collect_html_files': report_collector.collect_html_files,
        'derive_report_date': report_collector.derive_report_date,
        'refresh_reports': report_collector.refresh_reports,
        'resolve_outputs_path': report_collector.resolve_outputs_path,
        'find_files': report_collector.find_files,
    }


def _collect_for_dispatch(dispatch_type, outputs_dir, deps):
    find_files = deps['find_files']

    if dispatch_type == 'management':
        html_patterns = [
            'combined_management_report_*.html',
            'management_report_core_markets_*.html',
        ]
        attachment_patterns = ['combined_management_report_*.pdf', 'combined_management_report_*.xlsx']
    elif dispatch_type == 'core':
        html_patterns = ['management_report_core_markets_*.html']
        attachment_patterns = ['management_report_core_markets_*.pdf']
    else:
        html_patterns = ['management_report_usa_spa_*.html']
        attachment_patterns = []

    html_files = []
    seen_html = set()
    for pattern in html_patterns:
        for match in find_files(outputs_dir, pattern, 1):
            resolved = match.resolve()
            if resolved not in seen_html:
                html_files.append(match)
                seen_html.add(resolved)

    attachments = []
    seen_attach = set()
    for pattern in attachment_patterns:
        for match in find_files(outputs_dir, pattern, 1):
            resolved = match.resolve()
            if resolved not in seen_attach:
                attachments.append(match)
                seen_attach.add(resolved)

    return html_files, attachments


def _default_subject(dispatch_type, report_date, report_date_str):
    date_txt = report_date.strftime('%d.%m.%Y') if report_date else report_date_str()
    label = {
        'management': 'EOM Management Sales Report',
        'core': 'EOM Core Market Sales Report',
        'usa': 'EOM USA Sales Report',
    }[dispatch_type]
    return f"{label} {date_txt}"


def _extract_email(raw: str) -> str | None:
    if not raw:
        return None
    text = str(raw).strip()
    if not text:
        return None

    angle = re.search(r'<\s*([^>\s]+@[^>\s]+)\s*>', text)
    if angle:
        return angle.group(1).strip().lower()

    bare = re.search(r'([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})', text)
    if bare:
        return bare.group(1).strip().lower()
    return None


def _load_group_recipients(project_root: Path, json_path: str, group: str) -> tuple[list[str], bool]:
    path = Path(json_path)
    if not path.is_absolute():
        path = (project_root / path).resolve()
    if not path.exists():
        raise FileNotFoundError(f'Recipients JSON not found: {path}')

    payload = json.loads(path.read_text(encoding='utf-8'))
    if group not in payload:
        raise KeyError(f"Recipient group '{group}' not found in {path}")

    group_cfg = payload[group]
    enabled = bool(group_cfg.get('enabled', False))
    entries = group_cfg.get('recipients', [])

    normalized = []
    for entry in entries:
        email = _extract_email(str(entry))
        if email:
            normalized.append(email)

    deduped = list(dict.fromkeys(normalized))
    return deduped, enabled


def main(argv=None):
    args = parse_args(argv)

    project_root = Path(__file__).resolve().parents[1]
    load_dotenv(project_root / '.env')

    if args.sender:
        os.environ['REPORT_DISPATCH_GRAPH_SENDER'] = args.sender

    deps = _wire_dispatch_imports(project_root)

    recipients = []
    group_enabled = True

    if args.recipient_group:
        recipients, group_enabled = _load_group_recipients(project_root, args.recipients_json, args.recipient_group)
        if not recipients:
            raise ValueError(f"Recipient group '{args.recipient_group}' is empty")
        if not group_enabled and not args.dry_run:
            raise ValueError(
                f"Recipient group '{args.recipient_group}' is disabled in recipients JSON. "
                "Enable it first or use --dry-run for preview."
            )
    elif args.to:
        recipients = deps['parse_recipients'](args.to)
    else:
        raise ValueError('Provide either --to or --recipient-group')

    if not recipients:
        raise ValueError('No valid recipients resolved')

    if args.outputs_dir:
        outputs_dir = Path(args.outputs_dir)
        if not outputs_dir.is_absolute():
            outputs_dir = (project_root / outputs_dir).resolve()
        outputs_dir.mkdir(parents=True, exist_ok=True)
    else:
        outputs_dir = (project_root / 'data' / 'outputs').resolve()
        if not outputs_dir.exists():
            outputs_dir = deps['resolve_outputs_path']()

    if args.refresh:
        deps['refresh_reports'](outputs_dir)

    report_date = deps['derive_report_date'](outputs_dir)

    html_files, attachments = _collect_for_dispatch(args.dispatch_type, outputs_dir, deps)
    if not html_files:
        raise FileNotFoundError(f'No HTML files found for dispatch type {args.dispatch_type} in {outputs_dir}')

    body_type, body_content = deps['build_html_body'](
        html_files,
        args.body,
        banner_title=(
            f"Management Report (MTD: {report_date.strftime('%B')} 1-{report_date.day}, {report_date.year})"
            if report_date else deps['report_mtd_banner']()
        ),
        report_date=report_date,
    )

    if args.dispatch_type == 'core' and report_date:
        banner_title = f"Core Market Sales Report (MTD: {report_date.strftime('%B')} 1-{report_date.day}, {report_date.year})"
        body_type, body_content = deps['build_html_body'](
            html_files,
            args.body,
            banner_title=banner_title,
            report_date=report_date,
            footer_note='The PDF report is attached.',
        )
    elif args.dispatch_type == 'usa' and report_date:
        banner_title = f"USA Spa Sales Report (MTD: {report_date.strftime('%B')} 1-{report_date.day}, {report_date.year})"
        body_type, body_content = deps['build_html_body'](
            html_files,
            args.body,
            banner_title=banner_title,
            report_date=report_date,
            footer_note='',
        )

    subject = args.subject or _default_subject(args.dispatch_type, report_date, deps['report_date_str'])

    if args.recipient_group:
        LOG.info('Recipient group: %s (enabled=%s)', args.recipient_group, group_enabled)
    LOG.info('Dispatch recipients: %s', recipients)
    LOG.info('HTML body files (%d): %s', len(html_files), [p.name for p in html_files])
    LOG.info('Attachments (%d): %s', len(attachments), [p.name for p in attachments])
    LOG.info('Subject: %s', subject)

    if args.dry_run:
        LOG.info('Dry run enabled: email not sent')
        return

    deps['send_via_graph'](recipients, attachments, body_content, subject, body_type)
    LOG.info('Dispatch complete')


if __name__ == '__main__':
    main()
